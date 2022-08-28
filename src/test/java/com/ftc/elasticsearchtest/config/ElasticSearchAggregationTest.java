package com.ftc.elasticsearchtest.config;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.lang.Console;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.ftc.elasticsearchtest.entity.CarOrder;
import com.ftc.elasticsearchtest.entity.WebsiteRequest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * 网站索引名称
     */
    private static final String WEBSITE_INDEX = "website";

    /**
     * 测试数据
     */
    private static final List<CarOrder> CAR_ORDER_LIST;

    /*
      加载测试数据
     */
    static {
        String testData = "[{\"price\":10000,\"color\":\"red\",\"make\":\"honda\",\"soldDate\":\"2014-10-28\"},{\"price\":20000,\"color\":\"red\",\"make\":\"honda\",\"soldDate\":\"2014-11-05\"},{\"price\":30000,\"color\":\"green\",\"make\":\"ford\",\"soldDate\":\"2014-05-18\"},{\"price\":25000,\"color\":\"blue\",\"make\":\"ford\",\"soldDate\":\"2014-02-12\"},{\"price\":120000,\"color\":\"yellow\",\"make\":\"ford\",\"soldDate\":\"2014-06-11\"},{\"price\":90000,\"color\":\"yellow\",\"make\":\"ford\",\"soldDate\":\"2014-03-03\"},{\"price\":15000,\"color\":\"blue\",\"make\":\"toyota\",\"soldDate\":\"2014-07-02\"},{\"price\":12000,\"color\":\"green\",\"make\":\"toyota\",\"soldDate\":\"2014-08-19\"},{\"price\":80000,\"color\":\"red\",\"make\":\"bmw\",\"soldDate\":\"2014-01-01\"}]";
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
    void nested() {

        //1.同级嵌套，统计每种颜色，每种厂商的汽车销量
        SearchResponse<CarOrder> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("colorOrders", a -> a
                        .terms(t -> t.field("color")
                        ))
                .aggregations("makeOrders", a -> a
                        .terms(t -> t.field("make"))
                ), CarOrder.class
        );

        //2.校验
        Aggregate colorOrders = search.aggregations().get("colorOrders");
        List<StringTermsBucket> colorBuckets = colorOrders.sterms().buckets().array();
        Assert.isTrue(4 == colorBuckets.size());
        Aggregate makeOrders = search.aggregations().get("makeOrders");
        List<StringTermsBucket> makeBuckets = makeOrders.sterms().buckets().array();
        Assert.isTrue(4 == makeBuckets.size());

        //3.递归嵌套，统计取不同颜色汽车的销量，同时统计每种颜色汽车中每个制造商的销量以及每个制造商的总销售额
        search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("colorOrders", a -> a
                        .terms(t -> t.field("color"))
                        .aggregations("makeOrders", colorA -> colorA.terms(
                                        t -> t.field("make"))
                                .aggregations("makeTotalPrice", makeA -> makeA.sum(
                                        sum -> sum.field("price")
                                ))
                        )
                ), CarOrder.class
        );

        //4.校验
        colorOrders = search.aggregations().get("colorOrders");
        colorBuckets = colorOrders.sterms().buckets().array();
        Assert.isTrue(4 == colorBuckets.size());
        Console.log("按照颜色聚合-----");
        for (StringTermsBucket colorBucket : colorBuckets) {
            Console.log("颜色:" + colorBucket.key() + " 数量:" + colorBucket.docCount());
            Console.log("按照厂商聚合-----");
            makeOrders = colorBucket.aggregations().get("makeOrders");
            makeBuckets = makeOrders.sterms().buckets().array();
            for (StringTermsBucket makeBucket : makeBuckets) {
                Console.log("厂商:" + makeBucket.key() + " 数量:" + makeBucket.docCount());
                Aggregate makeTotalPrice = makeBucket.aggregations().get("makeTotalPrice");
                SumAggregate sum = makeTotalPrice.sum();
                Console.log("厂商销售总额:" + sum.value());
            }
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void histogram() {

        //1.按照销售额步长为20000统计不同颜色汽车的销量
        SearchResponse<CarOrder> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("priceCarOrders", a -> a
                        .histogram(h -> h
                                .field("price")
                                .interval((double) 20000)
                        )
                        .aggregations("colorOrders", ha -> ha
                                .terms(t -> t.field("color"))
                        )
                ), CarOrder.class
        );

        //2.校验
        Aggregate priceCarOrders = search.aggregations().get("priceCarOrders");
        List<HistogramBucket> priceCarOrdersBuckets = priceCarOrders.histogram().buckets().array();
        Assert.isTrue(7 == priceCarOrdersBuckets.size());
        Console.log("按照20000销售额价格聚合-----");
        for (HistogramBucket priceCarOrdersBucket : priceCarOrdersBuckets) {
            Console.log("[" + priceCarOrdersBucket.key() + "-" + (priceCarOrdersBucket.key() + (double) 20000)
                    + "]区间内销售量:" + priceCarOrdersBucket.docCount()
            );
            Aggregate colorOrders = priceCarOrdersBucket.aggregations().get("colorOrders");
            List<StringTermsBucket> colorBuckets = colorOrders.sterms().buckets().array();
            for (StringTermsBucket colorBucket : colorBuckets) {
                Console.log("颜色:" + colorBucket.key() + " 数量:" + colorBucket.docCount());
            }
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void DateHistogram() {

        //1.每个季度，不同汽车品牌的销售总额
        SearchResponse<CarOrder> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("quarterCarOrders", a -> a
                        .dateHistogram(d -> d
                                .field("soldDate")
                                .calendarInterval(CalendarInterval.Quarter)
                                .format(DatePattern.NORM_DATE_PATTERN)
                        )
                        .aggregations("makeOrders", da -> da
                                .terms(t -> t.field("make"))
                                .aggregations("totalPrice", ta -> ta
                                        .sum(sum -> sum.field("price"))
                                )
                        )
                ), CarOrder.class
        );

        //2.校验
        Aggregate quarterCarOrders = search.aggregations().get("quarterCarOrders");
        List<DateHistogramBucket> dateHistogramBuckets = quarterCarOrders.dateHistogram().buckets().array();
        Assert.isTrue(5 == dateHistogramBuckets.size());
        Console.log("按照季度聚合-----");
        for (DateHistogramBucket dateHistogramBucket : dateHistogramBuckets) {
            Console.log(dateHistogramBucket.keyAsString() + "季度售卖量" + dateHistogramBucket.docCount());
            Aggregate makeOrders = dateHistogramBucket.aggregations().get("makeOrders");
            List<StringTermsBucket> makeBuckets = makeOrders.sterms().buckets().array();
            for (StringTermsBucket makeBucket : makeBuckets) {
                Console.log("厂商" + makeBucket.key() + "售卖" + makeBucket.docCount() + "台");
                Aggregate totalPrice = makeBucket.aggregations().get("totalPrice");
                Console.log("共盈利" + totalPrice.sum().value());
            }
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void searchAndAggregationsFilter() {

        //1.获取福特汽车均价以及所有汽车均价，看看福特汽车的价格是否合理
        SearchResponse<CarOrder> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.term(t -> t
                        .field("make")
                        .value(FieldValue.of("ford"))
                ))))
                .aggregations("fortAvePrice", a -> a
                        .avg(avg -> avg.field("price"))
                )
                .aggregations("totalMake", a -> a
                        .global(g -> g)
                        .aggregations("totalAvgPrice", ta -> ta.
                                avg(avg -> avg.field("price"))
                        )
                ), CarOrder.class
        );

        //2.校验
        Aggregate fortAvePrice = search.aggregations().get("fortAvePrice");
        Aggregate totalMake = search.aggregations().get("totalMake");
        Assert.isTrue(4 == search.hits().hits().size());
        Console.log("福特汽车均价:" + fortAvePrice.avg().value());
        Console.log("所有汽车均价:" + totalMake.global().aggregations().get("totalAvgPrice").avg().value());
    }

    @Test
    @SneakyThrows(IOException.class)
    void searchNotFilterButAggregationsFilter() {

        //1.查询所有福特汽车订单，统计福特汽车中黄色汽车的销售总额
        SearchResponse<CarOrder> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.term(t -> t
                        .field("make")
                        .value(FieldValue.of("ford"))
                ))))
                .aggregations("yellowColorCarOrders", a -> a
                        .filter(f -> f.constantScore(c -> c.filter(cf -> cf.term(t -> t
                                .field("color")
                                .value(FieldValue.of("yellow"))
                        ))))
                        .aggregations("totalPrice", ta -> ta.sum(sum -> sum
                                .field("price")
                        ))
                ), CarOrder.class
        );

        //2.校验
        Assert.isTrue(4 == search.hits().hits().size());
        Aggregate yellowColorCarOrders = search.aggregations().get("yellowColorCarOrders");
        Assert.isTrue(2 == yellowColorCarOrders.filter().docCount());
        Aggregate totalPrice = yellowColorCarOrders.filter().aggregations().get("totalPrice");
        Console.log("福特黄色汽车的销售总额:" + totalPrice.sum().value());
    }

    @Test
    @SneakyThrows(IOException.class)
    void searchFilterButAggregationsNotFilter() {

        //1.查询展示蓝色福特汽车，同时统计不同颜色汽车的销售均价
        SearchResponse<CarOrder> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("colorCarOrders", a -> a.terms(t -> t
                                .field("color"))
                        .aggregations("avgPrice", fa -> fa.avg(avg -> avg
                                .field("price")
                        ))
                )
                .postFilter(pf -> pf.bool(b -> b
                        .must(m -> m.term(t -> t
                                .field("color")
                                .value(FieldValue.of("blue"))
                        ))
                        .must(m -> m.term(t -> t
                                .field("make")
                                .value(FieldValue.of("ford"))
                        )))
                ), CarOrder.class
        );

        //2.校验
        Assert.isTrue(1 == search.hits().hits().size());
        Aggregate colorCarOrders = search.aggregations().get("colorCarOrders");
        List<StringTermsBucket> colorBuckets = colorCarOrders.sterms().buckets().array();
        Assert.isTrue(4 == colorBuckets.size());

        for (StringTermsBucket colorBucket : colorBuckets) {
            Aggregate avePrice = colorBucket.aggregations().get("avgPrice");
            Console.log(colorBucket.key() + "汽车销售均价" + avePrice.avg().value());
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void sort() {

        //1.同级排序-统计所有厂商的销售总额，并明确了解谁的销售总额最高
        HashMap<String, SortOrder> sortMaps = MapUtil.newHashMap(1);
        sortMaps.put("totalPrice", SortOrder.Desc);
        SearchResponse<CarOrder> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("makeCarOrders", a -> a.terms(t -> t
                                        .field("make")
                                        .order(sortMaps)
                                )
                                .aggregations("totalPrice", fa -> fa.sum(sum -> sum
                                        .field("price")
                                ))
                ), CarOrder.class
        );

        //2.校验
        Aggregate makeCarOrders = search.aggregations().get("makeCarOrders");
        List<StringTermsBucket> makeBuckets = makeCarOrders.sterms().buckets().array();
        for (StringTermsBucket makeBucket : makeBuckets) {
            Aggregate totalPrice = makeBucket.aggregations().get("totalPrice");
            Console.log(makeBucket.key() + "汽车销售总额" + totalPrice.sum().value());
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void sortDeep() {

        //1.统计每个季度，每个厂商黄色汽车销售总额，并且直观看到黄色汽车销售总额高的厂
        HashMap<String, SortOrder> sortMaps = MapUtil.newHashMap(1);
        sortMaps.put("totalPrice>yellowTotalPrice", SortOrder.Desc);
        SearchResponse<CarOrder> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("quarterOrders", a -> a
                        .dateHistogram(d -> d
                                .field("soldDate")
                                .calendarInterval(CalendarInterval.Quarter)
                                .format(DatePattern.NORM_DATE_PATTERN)
                        )
                        .aggregations("makeOrders", da -> da
                                .terms(t -> t
                                        .field("make")
                                        .order(sortMaps)
                                )
                                .aggregations("totalPrice", daa -> daa
                                        .filter(f -> f.term(t -> t
                                                .field("color")
                                                .value(FieldValue.of("yellow"))
                                        ))
                                        .aggregations("yellowTotalPrice", daaa -> daaa.sum(sum -> sum
                                                .field("price")
                                        ))
                                )
                        )
                ), CarOrder.class
        );

        //2.校验
        Aggregate quarterOrders = search.aggregations().get("quarterOrders");
        List<DateHistogramBucket> histogramBuckets = quarterOrders.dateHistogram().buckets().array();
        Assert.isTrue(5 == histogramBuckets.size());
        for (DateHistogramBucket histogramBucket : histogramBuckets) {
            Console.log(histogramBucket.keyAsString() + "-季度共卖出" + histogramBucket.docCount() + "台汽车");
            Console.log("其中黄色汽车订单销售额如下:");
            Aggregate makeOrders = histogramBucket.aggregations().get("makeOrders");
            List<StringTermsBucket> makeBuckets = makeOrders.sterms().buckets().array();
            for (StringTermsBucket makeBucket : makeBuckets) {
                Aggregate totalPrice = makeBucket.aggregations().get("totalPrice");
                Aggregate yellowTotalPrice = totalPrice.filter().aggregations().get("yellowTotalPrice");
                Console.log(makeBucket.key() + "共卖出黄色汽车总价:" + yellowTotalPrice.sum().value());
            }
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void cardinality() {

        //1.统计共有多少个厂商的汽车订单
        SearchResponse<CarOrder> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .aggregations("makeCount", a -> a.cardinality(c -> c
                        .field("make")
                        .precisionThreshold(100)
                )), CarOrder.class
        );

        //2.校验
        Aggregate makeCount = search.aggregations().get("makeCount");
        Assert.isTrue(4 == makeCount.cardinality().value());
    }

    @Test
    @SneakyThrows(IOException.class)
    void percentiles() {

        //1.存储网站请求数据
        saveWebSite();

        //2.统计不同区域网站请求的平均值，通过百分数进行异常值分析，查看哪个区域网络延迟较高
        SearchResponse<WebsiteRequest> search = primaryClient.search(s -> s
                .index(WEBSITE_INDEX)
                .aggregations("zoneWebRequest", a -> a
                        .terms(t -> t.field("zone"))
                        .aggregations("avgLatency", ta -> ta.avg(avg -> avg
                                .field("latency"))
                        )
                        .aggregations("percentilesLatency", ta -> ta.percentiles(p -> p
                                .field("latency")
                                .percents((double) 5, (double) 25, (double) 50, (double) 75, (double) 100)
                        ))
                ), WebsiteRequest.class
        );

        //3.校验
        Aggregate zoneWebRequest = search.aggregations().get("zoneWebRequest");
        List<StringTermsBucket> zoneBuckets = zoneWebRequest.sterms().buckets().array();
        for (StringTermsBucket zoneBucket : zoneBuckets) {
            String key = zoneBucket.key();
            double avgLatency = zoneBucket.aggregations().get("avgLatency").avg().value();
            Console.log(key + "地区 网站平均响应延迟:" + avgLatency);

            TDigestPercentilesAggregate percentilesLatency = zoneBucket.aggregations().get("percentilesLatency").tdigestPercentiles();
            Percentiles values = percentilesLatency.values();
            for (String percentilesItem : values.keyed().keySet()) {
                Console.log(key + "地区" + percentilesItem + "%的请求 响应延迟:" + values.keyed().get(percentilesItem));
            }
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void percentileRanks() {

        //1.存储网站请求数据
        saveWebSite();

        //2.获取美国地区，网络延迟达到30,80,100的用户百分比
        SearchResponse<WebsiteRequest> search = secondaryClient.search(s -> s
                .index(WEBSITE_INDEX)
                .aggregations("usWebRequests", a -> a
                        .filter(f -> f.term(t -> t
                                .field("zone")
                                .value(FieldValue.of("US"))
                        ))
                        .aggregations("usPercentileRanks", fa -> fa.percentileRanks(p -> p
                                .field("latency")
                                .values((double) 30, (double) 80, (double) 100)
                        ))
                ), WebsiteRequest.class
        );

        //3.校验
        Aggregate usWebRequests = search.aggregations().get("usWebRequests");
        Aggregate usPercentileRanks = usWebRequests.filter().aggregations().get("usPercentileRanks");
        TDigestPercentileRanksAggregate tDigestPercentileRanks = usPercentileRanks.tdigestPercentileRanks();
        Map<String, String> keyed = tDigestPercentileRanks.values().keyed();
        Assert.isTrue(3 == keyed.size());
        for (String key : keyed.keySet()) {
            Console.log(keyed.get(key) + "%的用户 网络延迟请求达到了" + key + "毫秒");
        }
    }

    @SneakyThrows(IOException.class)
    void saveWebSite() {

        //1.判定索引是否存在
        BooleanResponse exists = primaryClient.indices().exists(e -> e.index(WEBSITE_INDEX));

        //2.不存在创建索引
        if (!exists.value()) {

            //3.创建索引
            CreateIndexResponse createIndexResponse = primaryClient.indices()
                    .create(c -> c
                            .index(WEBSITE_INDEX)
                            .mappings(m -> m
                                    .properties("latency", p -> p.long_(l -> l))
                                    .properties("timestamp", p -> p.date(d -> d))
                                    .properties("zone", p -> p.keyword(k -> k))
                            )
                    );
            Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
        }

        //4.清空数据
        DeleteByQueryResponse deleteByQueryResponse = primaryClient.deleteByQuery(d -> d
                .index(WEBSITE_INDEX)
                .query(q -> q.matchAll(m -> m))
                .refresh(true)
        );
        Assert.isTrue(ObjectUtil.isNotNull(deleteByQueryResponse.deleted()));

        //5.解析数据
        String dataStr = "[{\"latency\":100,\"zone\":\"US\",\"timestamp\":\"2014-10-28\"},{\"latency\":80,\"zone\":\"US\",\"timestamp\":\"2014-10-29\"},{\"latency\":99,\"zone\":\"US\",\"timestamp\":\"2014-10-29\"},{\"latency\":102,\"zone\":\"US\",\"timestamp\":\"2014-10-28\"},{\"latency\":75,\"zone\":\"US\",\"timestamp\":\"2014-10-28\"},{\"latency\":82,\"zone\":\"US\",\"timestamp\":\"2014-10-29\"},{\"latency\":100,\"zone\":\"EU\",\"timestamp\":\"2014-10-28\"},{\"latency\":280,\"zone\":\"EU\",\"timestamp\":\"2014-10-29\"},{\"latency\":155,\"zone\":\"EU\",\"timestamp\":\"2014-10-29\"},{\"latency\":623,\"zone\":\"EU\",\"timestamp\":\"2014-10-28\"},{\"latency\":380,\"zone\":\"EU\",\"timestamp\":\"2014-10-28\"},{\"latency\":319,\"zone\":\"EU\",\"timestamp\":\"2014-10-29\"}]";
        List<WebsiteRequest> websiteRequests = JSONUtil.toList(dataStr, WebsiteRequest.class);

        //6.封装Request
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.index(WEBSITE_INDEX);
        for (WebsiteRequest websiteRequest : websiteRequests) {
            builder.operations(o -> o.create(c -> c.document(websiteRequest)));
        }
        builder.refresh(Refresh.True);

        //7.存储数据
        BulkResponse bulk = primaryClient.bulk(builder.build());
        Assert.isFalse(bulk.errors());
    }
}
