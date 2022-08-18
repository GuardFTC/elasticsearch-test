package com.ftc.elasticsearchtest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-18 16:57:15
 * @describe: ElasticSearch配置类
 */
@Configuration
public class ElasticSearchConfig {

    @Primary
    @Bean(name = "primaryProperties")
    @ConfigurationProperties(prefix = "elasticsearch.primary")
    public ElasticSearchProperties primaryProperties() {
        return new ElasticSearchProperties();
    }

    @Primary
    @Bean(name = "secondaryProperties")
    @ConfigurationProperties(prefix = "elasticsearch.secondary")
    public ElasticSearchProperties secondaryProperties() {
        return new ElasticSearchProperties();
    }
}
