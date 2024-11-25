package com.migration.batchservice.config

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing

import org.springframework.batch.core.job.builder.JobBuilder
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.core.repository.JobRepository
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

@Configuration
@EnableBatchProcessing
class BatchConfig(
    private val dataSource: DataSource,
    private val transactionManager: PlatformTransactionManager,
    private val jobRepository: JobRepository
){


    @Bean
    fun job(): Job {
        return JobBuilder("archiveJob", jobRepository).incrementer(RunIdIncrementer()).start(step()).build()
    }

    @Bean
    fun step(): Step {
        return StepBuilder("archiveStep", jobRepository)
            .chunk<Map<String, Any>, Map<String, Any>>(100, transactionManager)
            .reader(reader())
            .processor(processor())
            .writer(writer())
            .build()
    }


    @Bean
    fun reader(): ItemReader<Map<String, Any>> {
        return JdbcCursorItemReaderBuilder<Map<String, Any>>()
            .dataSource(dataSource)
            .name("glDetailReader")
            .sql("SELECT * FROM archival_request")
            .rowMapper { rs, _ ->
                mapOf(
                    "ID" to rs.getLong("ID"),
                    "status" to rs.getString("status"),
                )
            }
            .build()
    }

    @Bean
    fun processor(): ItemProcessor<Map<String, Any>, Map<String, Any>> {
        return ItemProcessor { item ->
            // Process the item if
            println("Hi, I'm runningggggg")

            item
        }
    }

    @Bean
    fun writer(): ItemWriter<Map<String, Any>> {
        return ItemWriter { items ->
            // Send data to Azure Cosmos DB
//            val cosmosClient = CosmosClientBuilder()
//                .endpoint("your-cosmos-db-endpoint")
//                .key("your-cosmos-db-key")
//                .buildClient()
//            val container = cosmosClient.getDatabase("your-database").getContainer("your-container")
//            items.forEach { item ->
//                container.createItem(item, PartitionKey(item["partitionKey"].toString()), null)
//            }

//            val ids = items.map { it["id"] }
//            jdbcTemplate.update("DELETE FROM GLDetail WHERE id IN (?)", ids.joinToString(","))
//
//            // Check if there are more entries to process
//            val remainingEntries =
//                jdbcTemplate.queryForList("SELECT * FROM GLDetail WHERE type = ?", items.first()["type"])
//            if (remainingEntries.isEmpty()) {
//                // Delete the entry from archive_history table
//                jdbcTemplate.update("DELETE FROM archive_history WHERE id = ?", items.first()["id"])
//            }
            println("Hi, I'm runningggggg")
        }
    }
//
}


