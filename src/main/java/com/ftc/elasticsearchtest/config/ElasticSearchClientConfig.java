package com.ftc.elasticsearchtest.config;

import cn.hutool.core.util.StrUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.RequiredArgsConstructor;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-18 11:15:40
 * @describe: ElasticSearch客户端链接配置
 */
@Configuration
@RequiredArgsConstructor
public class ElasticSearchClientConfig {

    private final ElasticSearchConfig elasticSearchConfig;

    @Primary
    @Bean("primaryElasticsearchClient")
    public ElasticsearchClient getPrimaryRestClient() {

        //1.获取配置
        ElasticSearchProperties primaryProperties = elasticSearchConfig.primaryProperties();

        //2.生成客户端返回
        return getElasticsearchClient(primaryProperties);
    }

    @Bean("secondaryElasticsearchClient")
    public ElasticsearchClient getSecondaryRestClient() {

        //1.获取配置
        ElasticSearchProperties secondaryProperties = elasticSearchConfig.secondaryProperties();

        //2.生成客户端返回
        return getElasticsearchClient(secondaryProperties);
    }

    /**
     * 创建ElasticSearch客户端
     *
     * @param properties ElasticSearch配置
     * @return ElasticSearch客户端
     */
    private ElasticsearchClient getElasticsearchClient(ElasticSearchProperties properties) {

        //1.获取主机和端口
        String host = properties.getHost();
        Integer port = properties.getPort();

        //2.封装客户端初始参数
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port));

        //3.获取用户名密码
        String username = properties.getUsername();
        String password = properties.getPassword();

        //4.用户名密码判空，判定是否需要创建登录凭证
        if (StrUtil.isNotBlank(username) && StrUtil.isNotBlank(password)) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

            //5.设置用户名密码
            UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(username, password);
            credentialsProvider.setCredentials(AuthScope.ANY, credentials);

            //6.封装鉴权参数
            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        //7.创建Rest客户端
        RestClient restClient = builder.build();

        //8.生成Elasticsearch客户端链接
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        //9.生成Elasticsearch客户端并返回
        return new ElasticsearchClient(transport);
    }
}
