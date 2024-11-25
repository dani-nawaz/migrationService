package com.migration.batchservice.config

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing

import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.step.builder.StepBuilder
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
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
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.jdbc.support.JdbcTransactionManager

@Configuration
@EnableBatchProcessing
class BatchConfig(
    accessDataSource: DataSource
) : DefaultBatchConfiguration() {

    @Bean
    @Primary
    override fun getDataSource(): DataSource {
        // H2 datasource for Spring Batch metadata
        val dataSource = DataSourceBuilder.create()
            .driverClassName("org.h2.Driver")
            .url("jdbc:h2:mem:batchdb;DB_CLOSE_DELAY=-1;INIT=RUNSCRIPT FROM 'classpath:org/springframework/batch/core/schema-h2.sql'")
            .username("sa")
            .password("")
            .build()
        return dataSource
    }


    @Bean
    @Primary
    override fun getTransactionManager(): PlatformTransactionManager {
        return DataSourceTransactionManager(dataSource)
    }

    @Bean(name = ["accessTransactionManager"])
    fun accessTransactionManager(dataSource: DataSource): PlatformTransactionManager {
        return DataSourceTransactionManager(dataSource)
    }

    @Bean
    fun job(jobRepository: JobRepository, step: Step): Job {
        return JobBuilder("archiveJob", jobRepository)
            .incrementer(RunIdIncrementer())
            .start(step)
            .build()
    }

    @Bean
    fun step(
        jobRepository: JobRepository,
        @Qualifier("accessDataSource") accessDataSource: DataSource
    ): Step {
        return StepBuilder("archiveStep", jobRepository)
            .chunk<Map<String, Any>, Map<String, Any>>(100, getTransactionManager())
            .reader(reader(accessDataSource))
            .processor(processor())
            .writer(writer())
            .build()
    }

    @Bean
    fun reader(@Qualifier("accessDataSource") accessDataSource: DataSource): ItemReader<Map<String, Any>> {
        return JdbcCursorItemReaderBuilder<Map<String, Any>>()
            .dataSource(accessDataSource)
            .name("glDetailReader")
            .sql("SELECT * FROM archival_request")
            .rowMapper { rs, _ ->
                mapOf(
                    "ID" to rs.getLong("ID"),
                    "status" to rs.getString("status")
                )
            }
            .build()
    }

    @Bean
    fun processor(): ItemProcessor<Map<String, Any>, Map<String, Any>> {
        return ItemProcessor { item -> item }
    }

    @Bean
    fun writer(): ItemWriter<Map<String, Any>> {
        return ItemWriter { items ->
            println("Writing items: $items")
        }
    }
}