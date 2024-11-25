package com.migration.batchservice.service

import org.springframework.batch.core.Job
import org.springframework.batch.core.JobParametersBuilder
import org.springframework.batch.core.launch.JobLauncher
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.sql.Timestamp
import java.text.DateFormat

@Service
class ArchiveRequestService(
    private val jdbcTemplate: JdbcTemplate,
    private val jobLauncher: JobLauncher,
    private val job: Job
) {

    @Scheduled(fixedRate = 5000)
    fun checkForStatusEntry() {
 git        val sql = "SELECT * FROM archival_request WHERE status = 'pending'"
        val entries = jdbcTemplate.queryForList(sql)
        if (entries.isNotEmpty()) {
            entries.forEach { entry ->
                try {
                    // Update status to inProgress
                    jdbcTemplate.update(
                        "UPDATE archival_request SET status = 'inProgress' WHERE ID = ?",
                        entry["ID"]
                    )

                    println("Found entry with status 'entry': $entry")

                    val timestamp = entry["cut_off"] as Timestamp
                    val cutOffDate = timestamp.toLocalDateTime().toLocalDate()

                    val jobParameters = JobParametersBuilder()
                        .addString("type", entry["type"] as String)
                        .addString("cutOffDate", cutOffDate.toString())
                        .addLong("time", System.currentTimeMillis())
                        .toJobParameters()

                    jobLauncher.run(job, jobParameters)
                } catch (e: Exception) {
                    // Revert status back to pending if job launch fails
                    jdbcTemplate.update(
                        "UPDATE archival_request SET status = 'pending' WHERE ID = ?",
                        entry["ID"]
                    )
                    throw e
                }
            }
        }
    }
}
// TODO: flow complete,
// COPY TO AZuRE, local backup,
// DELETION