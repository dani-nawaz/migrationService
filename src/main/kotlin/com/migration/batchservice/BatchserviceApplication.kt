package com.migration.batchservice

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import javax.sql.DataSource
import org.springframework.boot.jdbc.DataSourceBuilder


@SpringBootApplication
class BatchserviceApplication

fun main(args: Array<String>) {
	runApplication<BatchserviceApplication>(*args)

}


//@Bean
//@Primary
//fun dataSource(): DataSource {
//	return DataSourceBuilder.create()
//		.url("jdbc:ucanaccess://E:/clickchain/demoNC24/demoNC24.mdb")
//		.driverClassName("net.ucanaccess.jdbc.UcanaccessDriver")
//		.build()
//}