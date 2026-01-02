package com.ph.dl.config;

import com.ph.dl.database.DatabaseManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.ph.dl")
public class AppConfig {

    @Bean
    public DatabaseManager databaseManager() {
        return DatabaseManager.getInstance();
    }
}
