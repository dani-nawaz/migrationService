package com.migration.batchservice

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@SpringBootApplication
class BatchserviceApplication

fun main(args: Array<String>) {
	runApplication<BatchserviceApplication>(*args)
}
