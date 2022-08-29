package com.ftc.elasticsearchtest.config;

import cn.hutool.core.collection.CollStreamUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.AggregationRange;
import co.elastic.clients.elasticsearch._types.aggregations.GeoHashGridBucket;
import co.elastic.clients.elasticsearch._types.aggregations.RangeBucket;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import com.ftc.elasticsearchtest.entity.Omission;
import com.ftc.elasticsearchtest.entity.TestShape;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.elasticsearch.core.geo.GeoJsonPoint;
import org.springframework.data.elasticsearch.core.geo.GeoJsonPolygon;
import org.springframework.data.geo.Point;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-29 09:35:30
 * @describe: ElasticSearch地理位置API单元测试
 */
@SpringBootTest
public class ElasticSearchGeoTest {

    @Resource
    @Qualifier("primaryElasticsearchClient")
    private ElasticsearchClient primaryClient;

    @Resource
    @Qualifier("secondaryElasticsearchClient")
    private ElasticsearchClient secondaryClient;

    /**
     * geo_point索引名称
     */
    private static final String GEO_POINT_INDEX_NAME = "omission";

    /**
     * geo_shape索引名称
     */
    private static final String GEO_SHAPE_INDEX_NAME = "geo_shape";

    /**
     * GEO_POINT数据
     */
    private static final String GEO_POINT_DATA_STR = "[{\"name\":\"北京\",\"location\":[116.2,39.56]},{\"name\":\"山东\",\"location\":[114.19,34.22]},{\"name\":\"湖北\",\"location\":[108.21,29.01]},{\"name\":\"湖南\",\"location\":[108.47,24.38]},{\"name\":\"云南\",\"location\":[97.31,21.8]},{\"name\":\"新疆\",\"location\":[86.37,42.45]}]";

    @BeforeEach
    @SneakyThrows(IOException.class)
    void createIndexAndSaveData() {

        //1.判定索引是否存在
        boolean exist = primaryClient.indices().exists(e -> e.index(GEO_POINT_INDEX_NAME)).value();

        //2.不存在创建索引
        if (!exist) {

            //3.创建索引
            CreateIndexResponse createIndexResponse = primaryClient
                    .indices()
                    .create(c -> c
                            .index(GEO_POINT_INDEX_NAME + "_1")
                            .aliases(GEO_POINT_INDEX_NAME, a -> a)
                            .mappings(m -> m
                                    .properties("name", p -> p.keyword(k -> k))
                                    .properties("location", p -> p.geoPoint(g -> g))
                            )
                            .settings(s -> s
                                    .refreshInterval(r -> r.time("1s"))
                                    .numberOfShards("3")
                                    .numberOfReplicas("1")
                            )
                    );

            //4.校验是否异常
            Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
        }

        //5.清空数据
        DeleteByQueryResponse deleteByQueryResponse = primaryClient.deleteByQuery(d -> d
                .index(GEO_POINT_INDEX_NAME)
                .query(q -> q.matchAll(m -> m))
                .refresh(Boolean.TRUE)
        );
        Assert.isTrue(0 == deleteByQueryResponse.failures().size());

        //6.构建新增数据
        List<Omission> omissions = JSONUtil.toList(GEO_POINT_DATA_STR, Omission.class);
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.index(GEO_POINT_INDEX_NAME);
        omissions.forEach(om -> builder.operations(o -> o.create(c -> c.document(om))));
        builder.refresh(Refresh.True);

        //7.新增数据
        BulkResponse bulk = primaryClient.bulk(builder.build());
        Assert.isFalse(bulk.errors());
    }

    @Test
    @SneakyThrows(IOException.class)
    void geoBoundingBox() {

        //1.以宁夏为左上角，台湾为右下角，可以获取到山东，湖南，湖北的相关信息
        SearchResponse<Omission> search = secondaryClient.search(s -> s
                .index(GEO_POINT_INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.geoBoundingBox(g -> g
                        .field("location")
                        .boundingBox(b -> b.tlbr(t -> t
                                .topLeft(tl -> tl.latlon(l -> l
                                        .lon(104.17)
                                        .lat(35.14)
                                ))
                                .bottomRight(br -> br.latlon(l -> l
                                        .lon(119.18)
                                        .lat(20.45)
                                ))
                        ))
                )))), Omission.class
        );

        //2.校验
        List<Hit<Omission>> hits = search.hits().hits();
        Assert.isTrue(3 == hits.size());
        List<Omission> omissions = CollStreamUtil.toList(search.hits().hits(), Hit::source);
        List<String> names = CollStreamUtil.toList(omissions, Omission::getName);
        Assert.isTrue(names.contains("山东"));
        Assert.isTrue(names.contains("湖北"));
        Assert.isTrue(names.contains("湖南"));
    }

    @Test
    @SneakyThrows(IOException.class)
    void geoDistance() {

        //1.以北京为圆心，获取2000km内的省份，理论上应该返回北京，山东，湖北，以及湖南4个省份
        SearchResponse<Omission> search = secondaryClient.search(s -> s
                .index(GEO_POINT_INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.geoDistance(g -> g
                        .field("location")
                        .location(l -> l.latlon(ll -> ll
                                .lon(116.2)
                                .lat(39.56)
                        ))
                        .distance("2000km")
                        .distanceType(GeoDistanceType.Arc)
                )))), Omission.class
        );

        //2.校验
        Assert.isTrue(4 == search.hits().hits().size());
        List<Omission> omissions = CollStreamUtil.toList(search.hits().hits(), Hit::source);
        List<String> names = CollStreamUtil.toList(omissions, Omission::getName);
        Assert.isTrue(names.contains("北京"));
        Assert.isTrue(names.contains("山东"));
        Assert.isTrue(names.contains("湖北"));
        Assert.isTrue(names.contains("湖南"));
    }

    @Test
    @SneakyThrows(IOException.class)
    void sort() {

        //1.以北京为圆心，获取2000km内的省份，并按照距离倒序 理论上返回顺序为湖南，湖北，山东，北京
        SearchResponse<Omission> search = secondaryClient.search(s -> s
                .index(GEO_POINT_INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.geoDistance(g -> g
                        .field("location")
                        .location(l -> l.latlon(ll -> ll
                                .lon(116.2)
                                .lat(39.56)
                        ))
                        .distance("2000km")
                ))))
                .sort(sort -> sort.geoDistance(g -> g
                        .field("location")
                        .location(l -> l.latlon(ll -> ll
                                .lon(116.2)
                                .lat(39.56)
                        ))
                        .order(SortOrder.Desc)
                        .unit(DistanceUnit.Kilometers)
                )), Omission.class
        );

        //2.校验
        Assert.isTrue(4 == search.hits().hits().size());
        Assert.isTrue("湖南".equals(search.hits().hits().get(0).source().getName()));
        Assert.isTrue("湖北".equals(search.hits().hits().get(1).source().getName()));
        Assert.isTrue("山东".equals(search.hits().hits().get(2).source().getName()));
        Assert.isTrue("北京".equals(search.hits().hits().get(3).source().getName()));
    }

    @Test
    @SneakyThrows(IOException.class)
    void aggregationGeoDistance() {

        //1.统计距离北京（包括北京）0~1000km范围、1000km到2000km范围内的省市数量
        SearchResponse<Omission> search = primaryClient.search(s -> s
                .index(GEO_POINT_INDEX_NAME)
                .aggregations("omissionNumber", a -> a.geoDistance(g -> g
                        .field("location")
                        .origin(o -> o.latlon(l -> l
                                .lon(116.2)
                                .lat(39.56)
                        ))
                        .ranges(
                                AggregationRange.of(ar -> ar.from("0").to("1000")),
                                AggregationRange.of(ar -> ar.from("1000").to("2000"))
                        )
                        .unit(DistanceUnit.Kilometers)
                        .distanceType(GeoDistanceType.Plane)
                )), Omission.class
        );

        //2.校验
        Aggregate omissionNumber = search.aggregations().get("omissionNumber");
        List<RangeBucket> rangeBuckets = omissionNumber.geoDistance().buckets().array();
        Assert.isTrue(2 == rangeBuckets.size());
        for (RangeBucket rangeBucket : rangeBuckets) {
            Assert.isTrue(2 == rangeBucket.docCount());
        }
    }

    @Test
    @SneakyThrows(IOException.class)
    void aggregationGeoHashGrid() {

        //1.网格聚合，指定geoHash长度为1
        SearchResponse<Omission> search = primaryClient.search(s -> s
                .index(GEO_POINT_INDEX_NAME)
                .aggregations("geoHashData", a -> a.geohashGrid(g -> g
                        .field("location")
                        .precision(p -> p.geohashLength(1))
                )), Omission.class
        );

        //2.校验
        Aggregate geoHashData = search.aggregations().get("geoHashData");
        List<GeoHashGridBucket> geoHashGridBuckets = geoHashData.geohashGrid().buckets().array();
        Assert.isTrue(2 == geoHashGridBuckets.size());
    }

    @Test
    @SneakyThrows(IOException.class)
    void aggregationGeoBounds() {

        //1.得到一个可以包含所有已存储数据的矩形
        SearchResponse<Omission> search = primaryClient.search(s -> s
                .index(GEO_POINT_INDEX_NAME)
                .aggregations("box", a -> a.geoBounds(g -> g
                        .field("location")
                )), Omission.class
        );

        //2.校验
        Aggregate box = search.aggregations().get("box");
        GeoBounds bounds = box.geoBounds().bounds();
        Assert.isTrue(bounds.isTlbr());
        double topLeftLon = bounds.tlbr().topLeft().latlon().lon();
        double topLeftLat = bounds.tlbr().topLeft().latlon().lat();
        double bottomRightLon = bounds.tlbr().bottomRight().latlon().lon();
        double bottomRightLat = bounds.tlbr().bottomRight().latlon().lat();
        Assert.isTrue(86.36999999172986 == topLeftLon);
        Assert.isTrue(42.44999995920807 == topLeftLat);
        Assert.isTrue(116.19999996386468 == bottomRightLon);
        Assert.isTrue(21.79999998304993 == bottomRightLat);
    }

    @Test
    @SneakyThrows(IOException.class)
    void geoShape() {

        //1.创建索引
        createIndexAndSaveShape();

        //2.查询甘肃黑龙江台湾三角形完全包含的，理论上返回北京点
        SearchResponse<TestShape> search = primaryClient.search(s -> s
                .index(GEO_SHAPE_INDEX_NAME)
                .query(q -> q.geoShape(g -> g
                        .field("location")
                        .shape(sh -> sh
                                .indexedShape(f -> f
                                        .index(GEO_SHAPE_INDEX_NAME)
                                        .id("ghtTriangleShape")
                                        .path("location")
                                )
                                .relation(GeoShapeRelation.Within)
                        )
                )), TestShape.class
        );

        //3.校验
        Assert.isTrue(1 == search.hits().hits().size());
    }

    @SneakyThrows(IOException.class)
    void createIndexAndSaveShape() {

        //1.判定索引是否存在
        boolean exist = primaryClient.indices().exists(e -> e.index(GEO_SHAPE_INDEX_NAME)).value();

        //2.不存在创建索引
        if (!exist) {

            //3.创建索引
            CreateIndexResponse createIndexResponse = primaryClient
                    .indices()
                    .create(c -> c
                            .index(GEO_SHAPE_INDEX_NAME)
                            .mappings(m -> m
                                    .properties("name", p -> p.keyword(k -> k))
                                    .properties("location", p -> p.geoShape(gs -> gs))
                                    .dynamic(DynamicMapping.False)
                            )
                    );

            //4.校验
            Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
        }

        //5.清空数据
        DeleteByQueryResponse deleteByQueryResponse = primaryClient.deleteByQuery(d -> d
                .index(GEO_SHAPE_INDEX_NAME)
                .query(q -> q.matchAll(m -> m))
                .refresh(Boolean.TRUE)
        );
        Assert.isTrue(0 == deleteByQueryResponse.failures().size());

        //6.准备数据(北京坐标点、甘肃黑龙江台湾三角形、宁夏新疆西藏三角形)
        GeoJsonPoint point = GeoJsonPoint.of(116.20, 39.56);
        TestShape beijingShape = new TestShape();
        beijingShape.setName("北京点");
        beijingShape.setLocation(JSONUtil.parseObj(point.toJson()));

        GeoJsonPolygon ghtTriangle = GeoJsonPolygon.of(CollUtil.newArrayList(
                new Point(92.13, 32.31),
                new Point(125.03, 50.49),
                new Point(119.18, 20.45),
                new Point(92.13, 32.31)
        ));
        TestShape ghtTriangleShape = new TestShape();
        ghtTriangleShape.setName("甘肃黑龙江台湾三角形");
        ghtTriangleShape.setLocation(JSONUtil.parseObj(ghtTriangle.toJson()));

        GeoJsonPolygon nxxTriangle = GeoJsonPolygon.of(CollUtil.newArrayList(
                new Point(105.49, 38.08),
                new Point(96.37, 42.45),
                new Point(80.24, 31.29),
                new Point(105.49, 38.08)
        ));
        TestShape nxxTriangleShape = new TestShape();
        nxxTriangleShape.setName("宁夏新疆西藏三角形");
        nxxTriangleShape.setLocation(JSONUtil.parseObj(nxxTriangle.toJson()));

        //7.存入数据
        BulkResponse bulk = primaryClient.bulk(b -> b
                .index(GEO_SHAPE_INDEX_NAME)
                .operations(o -> o.create(c -> c.document(beijingShape).id("beijingShape")))
                .operations(o -> o.create(c -> c.document(ghtTriangleShape).id("ghtTriangleShape")))
                .operations(o -> o.create(c -> c.document(nxxTriangleShape).id("nxxTriangleShape")))
                .refresh(Refresh.True)
        );
        Assert.isFalse(bulk.errors());
    }
}
