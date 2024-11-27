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

    data class TableConfig(
        val tableName: String,
        val selectClause: String,
        val fromClause: String,
        val whereClause: String,
        val deleteQuery: String,
        val sortKey: String = "ID"
    )

    private val tableConfigs = mapOf(
        "GLDETAIL" to TableConfig(
            tableName = "tblGJBatch",
            selectClause = "ID",
            fromClause = "tblGJBatch a INNER JOIN tblGJBatchDetail b ON a.BatchNum = b.BatchNum",
            whereClause = "b.PostPeriod <= ?",
            deleteQuery = "DELETE FROM tblGjBatch WHERE ID = ?",
            sortKey = "ID"
        )
    )


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
    fun job(jobRepository: JobRepository, @Qualifier("accessDataSource") accessDataSource: DataSource): Job {

        val jobBuilder = JobBuilder("archiveJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .start(createStepForTable("GLDETAIL", jobRepository, accessDataSource))

//        tableConfigs.forEach { (tableType, _) ->
//            val steps = createStepForTable(tableType, jobRepository, accessDataSource)
//            jobBuilder.next(steps)
//        }
        return jobBuilder.build()
    }

    fun createStepForTable(
        tableType: String,
        jobRepository: JobRepository,
        accessDataSource: DataSource
    ): Step {
        val config = tableConfigs[tableType] ?: throw IllegalArgumentException("Unknown table type: $tableType")
        return StepBuilder("delete${config.tableName}Step", jobRepository)
            .chunk<Map<String, Any>, Long>(100, transactionManager)
            .reader(pagingReader(accessDataSource, config))
            .processor(ItemProcessor<Map<String, Any>, Long> { item -> item["ID"] as Long })
            .writer(deleteWriter(accessDataSource, config))
            .build()
    }

    @StepScope
    fun getCutOffDate(): String? {
        val stepContext = StepSynchronizationManager.getContext()
        val cutoffdate = stepContext?.stepExecution?.jobExecution?.jobParameters?.getString("cutOffDate")
        return cutoffdate
    }

    fun pagingReader(
        accessDataSource: DataSource,
        tableConfig: TableConfig
    ): JdbcPagingItemReader<Map<String, Any>> {
        val cutoffdate = getCutOffDate()
        val reader = JdbcPagingItemReader<Map<String, Any>>()
        reader.setDataSource(accessDataSource)
        reader.pageSize = 100
        reader.setRowMapper { rs, _ ->
            mapOf(
                "ID" to rs.getLong("ID")
            )
        }
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
        reader.afterPropertiesSet()
        return reader
    }



    fun deleteWriter(accessDataSource: DataSource, tableConfig: TableConfig): ItemWriter<Long> {
        var delete_query = tableConfig.deleteQuery
        val jdbcTemplate = JdbcTemplate(accessDataSource)
        return ItemWriter { items ->
            items.forEach { id ->
                jdbcTemplate.update(delete_query, id)
            }
            logger.info("Deleted ${items.size()} items successfully")
        }
    }

    @Bean
    fun stepExecutionListener(): StepExecutionListener {
        return object : StepExecutionListener {
            override fun beforeStep(stepExecution: StepExecution) {
                val type = stepExecution.jobParameters.getString("cutoffdate")
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