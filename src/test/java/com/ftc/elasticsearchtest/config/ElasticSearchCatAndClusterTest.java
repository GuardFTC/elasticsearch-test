package com.ftc.elasticsearchtest.config;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ObjectUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.HealthStatus;
import co.elastic.clients.elasticsearch.cat.*;
import co.elastic.clients.elasticsearch.cat.aliases.AliasesRecord;
import co.elastic.clients.elasticsearch.cat.health.HealthRecord;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.cat.master.MasterRecord;
import co.elastic.clients.elasticsearch.cat.nodes.NodesRecord;
import co.elastic.clients.elasticsearch.cluster.ElasticsearchClusterClient;
import co.elastic.clients.elasticsearch.cluster.StateResponse;
import co.elastic.clients.json.JsonData;
import jakarta.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

@SpringBootTest
class ElasticSearchCatAndClusterTest {

    @Resource
    @Qualifier("primaryElasticsearchClient")
    private ElasticsearchClient primaryClient;

    @Test
    void testCat() throws IOException {

        //1.获取cat客户端
        ElasticsearchCatClient cat = primaryClient.cat();

        //2.获取集群健康状况
        HealthResponse health = cat.health();
        List<HealthRecord> healthRecords = health.valueBody();

        //3.结果校验
        Assert.isTrue(ObjectUtil.isNotNull(healthRecords));
        Assert.isTrue(1 == healthRecords.size());
        HealthRecord healthRecord = healthRecords.get(0);
        Assert.isTrue("ftc-es".equals(healthRecord.cluster()));
        Assert.isTrue("3".equals(healthRecord.nodeTotal()));

        //4.查询主节点信息并校验结果
        MasterResponse master = cat.master();
        List<MasterRecord> masterRecords = master.valueBody();
        Assert.isTrue(ObjectUtil.isNotNull(masterRecords));
        Assert.isTrue(1 == masterRecords.size());

        //5.查询集群节点信息
        NodesResponse nodes = cat.nodes();
        List<NodesRecord> nodesRecords = nodes.valueBody();
        Assert.isTrue(ObjectUtil.isNotNull(nodesRecords));
        Assert.isTrue(3 == nodesRecords.size());

        //6.查询集群别名信息
        AliasesResponse aliases = cat.aliases();
        List<AliasesRecord> aliasesRecords = aliases.valueBody();
        Assert.isTrue(ObjectUtil.isNotNull(aliasesRecords));

        //7.查询集群索引信息
        IndicesResponse indices = cat.indices();
        List<IndicesRecord> indicesRecords = indices.valueBody();
        Assert.isTrue(ObjectUtil.isNotNull(indicesRecords));
    }

    @Test
    void testCluster() throws IOException {

        //1.获取cluster客户端
        ElasticsearchClusterClient cluster = primaryClient.cluster();

        //2.查询集群健康状态
        co.elastic.clients.elasticsearch.cluster.HealthResponse health = cluster.health();

        //3.校验集群健康状态信息
        Assert.isTrue(ObjectUtil.isNotNull(health));
        Assert.isTrue("ftc-es".equals(health.clusterName()));
        Assert.isTrue(3 == health.numberOfNodes());
        Assert.isTrue(HealthStatus.Green.jsonValue().equals(health.status().jsonValue()));

        //4.查询集群状态
        StateResponse state = cluster.state();
        JsonData jsonData = state.valueBody();

        //5.校验集群状态
        Assert.isTrue(ObjectUtil.isNotNull(jsonData));
        JsonObject status = jsonData.toJson().asJsonObject();
        Assert.isTrue("ftc-es".equals(status.getString("cluster_name")));
        Assert.isTrue(3 == status.getJsonObject("nodes").size());
    }
}