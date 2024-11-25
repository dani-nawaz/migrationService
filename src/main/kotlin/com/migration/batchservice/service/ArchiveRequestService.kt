package com.migration.batchservice.service

import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
@Service
class ArchiveRequestService(private val jdbcTemplate: JdbcTemplate,
                            private val jobLauncher: JobLauncher,
                            private val job: Job) {

    @Scheduled(fixedRate = 5000)
    fun checkForStatusEntry() {
        val sql = "SELECT * FROM archival_request WHERE status = 'Pending'"
        val entries = jdbcTemplate.queryForList(sql)
        if (entries.isNotEmpty()) {

            entries.forEach { entry ->
                println("Found entry with status 'entry': $entry")


            }
        }

    }
}