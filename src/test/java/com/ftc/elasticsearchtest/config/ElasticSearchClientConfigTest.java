package com.ftc.elasticsearchtest.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;

@SpringBootTest
class ElasticSearchClientConfigTest {

    @Resource
    @Qualifier("primaryElasticsearchClient")
    private ElasticsearchClient primaryClient;

    @Resource
    @Qualifier("secondaryElasticsearchClient")
    private ElasticsearchClient secondaryClient;

    @Test
    void testCreateIndex() throws IOException {
        primaryClient.indices().delete(c -> c.index("test_java"));
        secondaryClient.indices().create(c -> c.index("test_java"));
    }
}