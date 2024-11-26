package com.migration.batchservice.config

import org.slf4j.LoggerFactory
import org.springframework.batch.core.*
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean
import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import javax.sql.DataSource
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration
import org.springframework.batch.core.repository.JobRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.jdbc.DataSourceBuilder
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.datasource.DataSourceTransactionManager

import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.scope.context.StepSynchronizationManager
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.JdbcPagingItemReader
import org.springframework.batch.item.database.Order
import org.springframework.batch.item.database.support.AbstractSqlPagingQueryProvider
import org.springframework.jdbc.core.JdbcTemplate


@Configuration
@EnableBatchProcessing
class BatchConfig : DefaultBatchConfiguration() {



    @Bean
    @Primary
    override fun getDataSource(): DataSource {
        val dataSource = DataSourceBuilder.create().driverClassName("org.h2.Driver")
            .url("jdbc:h2:mem:batchdb;DB_CLOSE_DELAY=-1;INIT=RUNSCRIPT FROM 'classpath:org/springframework/batch/core/schema-h2.sql'")
            .username("sa").password("").build()
        return dataSource
    }


    @Bean
    @Primary
    override fun getTransactionManager(): PlatformTransactionManager {
        return DataSourceTransactionManager(dataSource)
    }

    @Bean
    fun job(jobRepository: JobRepository, step: Step, deleteStep: Step): Job {
        return JobBuilder("archiveJob", jobRepository).incrementer(RunIdIncrementer()).start(step).next(deleteStep)
            .build()
    }

    @Bean
    fun deleteStep(
        jobRepository: JobRepository,
        @Qualifier("accessDataSource") accessDataSource: DataSource
    ): Step {
        return StepBuilder("deleteEntriesStep", jobRepository)
            .chunk<Map<String, Any>, Long>(100, transactionManager)
            .reader(pagingReader(accessDataSource))
            .processor(deleteProcessor())
            .writer(deleteWriter(accessDataSource))
            .build()
    }

    @Bean
    @StepScope
    fun deleteReader(@Qualifier("accessDataSource") accessDataSource: DataSource): JdbcCursorItemReader<Map<String, Any>> {
        val stepContext = StepSynchronizationManager.getContext()
        val cutoffdate = stepContext?.stepExecution?.jobExecution?.jobParameters?.getString("cutOffDate")
        return JdbcCursorItemReaderBuilder<Map<String, Any>>()
            .dataSource(accessDataSource)
            .name("deleteReader")
            .sql("SELECT ID FROM archival_request WHERE cut_off = ?")
            .preparedStatementSetter { ps -> ps.setString(1, cutoffdate) }
            .rowMapper { rs, _ -> mapOf("ID" to rs.getLong("ID")) }
            .build()
    }

    @Bean
    @StepScope
    fun pagingReader(
        @Qualifier("accessDataSource") accessDataSource: DataSource
    ): JdbcPagingItemReader<Map<String, Any>> {
        val stepContext = StepSynchronizationManager.getContext()
        val cutoffdate = stepContext?.stepExecution?.jobExecution?.jobParameters?.getString("cutOffDate")
        val reader = JdbcPagingItemReader<Map<String, Any>>()
        reader.setDataSource(accessDataSource)
        reader.pageSize = 100
        reader.setRowMapper { rs, _ ->
            mapOf(
                "ID" to rs.getLong("ID")
            )
        }

        // Create a custom query provider
        class CustomPagingQueryProvider : AbstractSqlPagingQueryProvider() {
            override fun generateFirstPageQuery(pageSize: Int): String {
                return "SELECT ${selectClause} FROM ${fromClause} WHERE ${whereClause} ORDER BY ${sortKeys.keys.first()} LIMIT $pageSize"
            }

            override fun generateRemainingPagesQuery(pageSize: Int): String {
                return "SELECT ${selectClause} FROM ${fromClause} WHERE ${whereClause} AND ${sortKeys.keys.first()} > ? ORDER BY ${sortKeys.keys.first()} LIMIT $pageSize"
            }

            fun generateJumpToItemQuery(itemIndex: Int, pageSize: Int): String {
                return "SELECT ${selectClause} FROM ${fromClause} WHERE ${whereClause} ORDER BY ${sortKeys.keys.first()} LIMIT $pageSize OFFSET $itemIndex"
            }

            fun generateJumpToItemQueryForUpdate(itemIndex: Int, pageSize: Int): String {
                return generateJumpToItemQuery(itemIndex, pageSize)
            }
        }

        val queryProvider = CustomPagingQueryProvider().apply {
            setSelectClause("ID")
            setFromClause("tblGjBatch a INNER JOIN tblGjBatchDetail b ON a.BatchNum = b.BatchNum")
            setWhereClause("b.PostPeriod <= '$cutoffdate'")
            sortKeys = mapOf("ID" to Order.ASCENDING)
        }

        reader.setQueryProvider(queryProvider)
//        reader.setParameterValues(mapOf("cutoffdate" to cutoffdate))
        println("Read first batch")

        return reader
    }
    @Bean
    fun deleteProcessor(): ItemProcessor<Map<String, Any>, Long> {
        return ItemProcessor { item -> item["ID"] as Long }
    }

    @Bean
    fun deleteWriter(@Qualifier("accessDataSource") accessDataSource: DataSource): ItemWriter<Long> {
        val jdbcTemplate = JdbcTemplate(accessDataSource)
        return ItemWriter { items ->
            items.forEach { id ->
                jdbcTemplate.update("DELETE FROM TBLGLDETAIL WHERE ID = ?", id)
            }
            logger.info("Deleted ${items.size()} items successfully")
        }
    }

    @Bean
    fun stepExecutionListener(): StepExecutionListener {
        return object : StepExecutionListener {
            override fun beforeStep(stepExecution: StepExecution) {
                val type = stepExecution.jobParameters.getString("type")
                stepExecution.executionContext.putString("type", type)
            }

            override fun afterStep(stepExecution: StepExecution): ExitStatus? {
                return stepExecution.exitStatus
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(BatchConfig::class.java)
    }
}

//DELETE tblGJBatch
//FROM tblGJBatch
//INNER JOIN tblGJBatchDetail ON tblGJBatch.BatchNum = tblGJBatchDetail.BatchNum
//WHERE tblGJBatchDetail.PostPeriod <= ?

//SELECT ID FROM tblGjBatch inner join tblGjBatchDetail on tblGjBatch.BatchNum = tblGjBatchDetail.BatchNum where tblGjBatchDetail.PostPeriod <= ?

//DELETE FROM TblGJBatch WHERE ID = ?