package com.ftc.elasticsearchtest.config;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.lang.Console;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Buckets;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import com.ftc.elasticsearchtest.entity.CarOrder;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-24 19:35:36
 * @describe: ElasticSearch聚合操作Api
 */
@SpringBootTest
public class ElasticSearchAggregationTest {

    @Resource
    @Qualifier("primaryElasticsearchClient")
    private ElasticsearchClient primaryClient;

    @Resource
    @Qualifier("secondaryElasticsearchClient")
    private ElasticsearchClient secondaryClient;

    /**
     * 测试Mock索引名称常量
     */
    private static final String INDEX_NAME = "test_car_order";

    /**
     * 测试数据
     */
    private static final List<CarOrder> CAR_ORDER_LIST;

    /*
      加载测试数据
     */
    static {
        String testData = "[{\"price\":10000,\"color\":\"red\",\"make\":\"honda\",\"soldDate\":\"2014-10-28\"},{\"price\":20000,\"color\":\"red\",\"make\":\"honda\",\"soldDate\":\"2014-11-05\"},{\"price\":30000,\"color\":\"green\",\"make\":\"ford\",\"soldDate\":\"2014-05-18\"},{\"price\":15000,\"color\":\"blue\",\"make\":\"toyota\",\"soldDate\":\"2014-07-02\"},{\"price\":12000,\"color\":\"green\",\"make\":\"toyota\",\"soldDate\":\"2014-08-19\"},{\"price\":20000,\"color\":\"red\",\"make\":\"honda\",\"soldDate\":\"2014-11-05\"},{\"price\":80000,\"color\":\"red\",\"make\":\"bmw\",\"soldDate\":\"2014-01-01\"},{\"price\":25000,\"color\":\"blue\",\"make\":\"ford\",\"soldDate\":\"2014-02-12\"},{\"price\":120000,\"color\":\"yellow\",\"make\":\"ford\",\"soldDate\":\"2014-06-11\"},{\"price\":90000,\"color\":\"yellow\",\"make\":\"ford\",\"soldDate\":\"2014-03-03\"}]";
        CAR_ORDER_LIST = JSONUtil.toList(JSONUtil.parseArray(testData), CarOrder.class);
    }

    @BeforeEach
    void saveTestData() throws IOException {

        //1.判定索引是否存在
        boolean exist = primaryClient.indices().exists(e -> e.index(INDEX_NAME)).value();

        //2.不存在创建索引
        if (!exist) {

            //3.创建索引
            CreateIndexResponse createIndexResponse = primaryClient.indices().create(c -> c
                    .index(INDEX_NAME + "_1")
                    .aliases(INDEX_NAME, a -> a)
                    .mappings(m -> m
                            .properties("price", p -> p.double_(d -> d))
                            .properties("color", p -> p.keyword(k -> k))
                            .properties("make", p -> p.keyword(k -> k))
                            .properties("soldDate", p -> p.date(d -> d))
                    )
                    .settings(s -> s
                            .refreshInterval(r -> r.time("1s"))
                            .numberOfShards("3")
                            .numberOfReplicas("1")
                    )
            );
            Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
        }

        //4.删除数据
        DeleteByQueryResponse deleteByQuery = primaryClient.deleteByQuery(d -> d
                .index(INDEX_NAME)
                .query(q -> q.matchAll(m -> m))
                .refresh(true)
        );
        Assert.isTrue(ObjectUtil.isNotNull(deleteByQuery.deleted()));

        //5.批量存入数据
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.index(INDEX_NAME);
        CAR_ORDER_LIST.forEach(car -> builder.operations(o -> o.create(c -> c.document(car))));
        builder.refresh(Refresh.True);
        BulkRequest build = builder.build();

        //6.存储数据
        BulkResponse bulk = primaryClient.bulk(build);
        Assert.isFalse(bulk.errors());
    }

    @Test
    @SneakyThrows(IOException.class)
    void term() {

        //1.根据颜色进行聚合
        SearchResponse<CarOrder> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("colorCars", a -> a.terms(t -> t
                        .field("color"))
                ), CarOrder.class
        );

        //2.校验
        Aggregate colorCars = search.aggregations().get("colorCars");
        Buckets<StringTermsBucket> buckets = colorCars.sterms().buckets();
        Assert.isTrue(4 == buckets.array().size());
        buckets.array().forEach(b -> {
            Console.log(b.key());
            Console.log(b.docCount());
        });
    }

    @Test
    @SneakyThrows(IOException.class)
    void nested(){

        //1.同级嵌套


    }
}
