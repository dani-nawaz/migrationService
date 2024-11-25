package com.migration.batchservice.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import javax.sql.DataSource

@Configuration
class DataSourceConfig {

    @Bean(name = ["accessDataSource"])
    fun accessDataSource(): DataSource {
        val dataSource = DriverManagerDataSource()
        dataSource.setDriverClassName("net.ucanaccess.jdbc.UcanaccessDriver")
        dataSource.url = "jdbc:ucanaccess://E:\\clickchain\\demoNC24\\demoNC24.mdb"
        return dataSource
    }

    @Bean
    fun jdbcTemplate(): JdbcTemplate {
        val dataSource = DriverManagerDataSource()
        dataSource.setDriverClassName("net.ucanaccess.jdbc.UcanaccessDriver")
        dataSource.url = "jdbc:ucanaccess://D:\\clickchain\\gms db\\demoNC24.mdb"
        return JdbcTemplate(accessDataSource())
    }
}