package com.ftc.elasticsearchtest.config;

import cn.hutool.core.collection.CollStreamUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.json.JsonData;
import com.ftc.elasticsearchtest.entity.SearchStudent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-22 15:32:17
 * @describe: ElasticSearch查询单元测试
 */
@SpringBootTest
public class ElasticSearchSearchTest {

    @Resource
    @Qualifier("primaryElasticsearchClient")
    private ElasticsearchClient primaryClient;

    @Resource
    @Qualifier("secondaryElasticsearchClient")
    private ElasticsearchClient secondaryClient;

    /**
     * 测试Mock索引名称常量
     */
    private static final String INDEX_NAME = "test_index";

    /**
     * 测试数据
     */
    private static final String TEST_DATA = "[{\"name\":\"王乾雯\",\"age\":\"18\",\"grade\":16.7,\"des\":\"好看性感的小姐姐\",\"tags\":[\"性感\",\"风骚\",\"很好上\"]},{\"name\":\"宋可心\",\"age\":\"18\",\"grade\":22.7,\"des\":\"她也是一个性感小姐姐\",\"tags\":[\"性感\",\"声音好听\",\"美脚\"]},{\"name\":\"郭楠\",\"age\":\"28\",\"grade\":15.2,\"des\":\"长腿小姐姐\",\"tags\":[\"腿长\"]},{\"name\":\"王可菲\",\"age\":\"17\",\"grade\":68.2,\"des\":\"清纯好看的小姐姐\",\"tags\":[\"高颜值\"]},{\"name\":\"王彦舒\",\"age\":\"22\",\"grade\":11.2,\"des\":\"闷骚的小姐姐\",\"tags\":[\"可爱\",\"闷骚\"]},{\"name\":\"宁娜\",\"age\":\"25\",\"grade\":44.4,\"des\":\"特别骚的一个小姐姐\",\"tags\":[\"骚\",\"长腿\",\"我想上她\",\"性感\"]},{\"name\":\"lulu\",\"age\":\"19\",\"grade\":16.4,\"des\":\"脚特别好看的小姐姐\",\"tags\":[\"骚\",\"长腿\",\"我想上她\",\"极品美脚\",\"性感\"]}]";

    @BeforeEach
    void createIndexAndAddData() throws IOException {

        //1.判定索引是否存在
        boolean exist = primaryClient.indices().exists(e -> e.index(INDEX_NAME)).value();

        //2.不存在创建索引
        if (!exist) {

            //3.创建索引
            CreateIndexResponse createIndexResponse = primaryClient.indices().create(c -> c
                    .index(INDEX_NAME + "_1")
                    .aliases(INDEX_NAME, a -> a)
                    .mappings(m -> m
                            .properties("name", p -> p.keyword(k -> k))
                            .properties("age", p -> p.integer(in -> in))
                            .properties("grade", p -> p.halfFloat(s -> s))
                            .properties("des", p -> p.text(t -> t.analyzer("ik_max_word")))
                            .properties("tags", p -> p.keyword(k -> k))
                    )
                    .settings(s -> s
                            .refreshInterval(t -> t.time("1s"))
                            .numberOfReplicas("1")
                            .numberOfShards("3")
                    )
            );
            Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
        }

        //4.获取索引中文档数量
        long count = primaryClient.count(c -> c.index(INDEX_NAME)).count();
        if (0 != count) {

            //5.文档数据不为空，全量删除
            DeleteByQueryResponse deleteByQueryResponse = primaryClient.deleteByQuery(d -> d
                    .index(INDEX_NAME)
                    .query(q -> q.matchAll(m -> m))
                    .refresh(true)
            );
            Assert.isTrue(ObjectUtil.isNotNull(deleteByQueryResponse.deleted()));
        }

        //6.准备测试数据
        List<SearchStudent> students = JSONUtil.toList(TEST_DATA, SearchStudent.class);
        BulkRequest.Builder builder = new BulkRequest.Builder();
        builder.index(INDEX_NAME);
        students.forEach(s -> builder.operations(o -> o.create(c -> c.document(s))));
        builder.refresh(Refresh.True);

        //7.存入数据
        BulkResponse bulk = primaryClient.bulk(builder.build());
        Assert.isFalse(bulk.errors());
        Assert.isTrue(7 == bulk.items().size());
    }

    @Test
    void term() throws IOException {

        //1.查询名称为宋可心的同学
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.term(t -> t
                        .field("name")
                        .value(FieldValue.of("宋可心"))
                )))), SearchStudent.class
        );

        //2.验证
        Assert.isTrue(1 == search.hits().hits().size());
        Assert.isTrue("宋可心".equals(search.hits().hits().get(0).source().getName()));
    }

    @Test
    void terms() throws IOException {

        //1.查询名称为宋可心和王乾雯的同学
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.terms(t -> t
                        .field("name")
                        .terms(terms -> terms.value(CollUtil.newArrayList(
                                FieldValue.of("宋可心"),
                                FieldValue.of("王乾雯")
                        )))
                )))), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(2 == search.hits().hits().size());
        List<String> names = CollUtil.newArrayList(
                search.hits().hits().get(0).source().getName(),
                search.hits().hits().get(1).source().getName()
        );
        Assert.isTrue(names.contains("王乾雯"));
        Assert.isTrue(names.contains("宋可心"));
    }

    @Test
    void range() throws IOException {

        //1.查询年龄小于18岁的同学
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.range(r -> r
                        .field("age")
                        .lt(JsonData.of(18))
                )), SearchStudent.class
        );
        Assert.isTrue(1 == search.hits().hits().size());
        Assert.isTrue(17 == search.hits().hits().get(0).source().getAge());

        //2.查询年龄大于17，小于23的同学
        search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.range(r -> r
                        .field("age")
                        .gt(JsonData.of(17))
                        .lt(JsonData.of(23))
                )), SearchStudent.class
        );
        Assert.isTrue(4 == search.hits().hits().size());
    }

    @Test
    void exist() throws IOException {

        //1.判定文档不包含对应字段
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.exists(e -> e
                        .field("qqqqq")
                )))), SearchStudent.class
        );
        Assert.isTrue(0 == search.hits().hits().size());

        //2.判定文档包含对应字段
        search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.exists(e -> e
                        .field("age")
                )))), SearchStudent.class
        );
        Assert.isTrue(7 == search.hits().hits().size());
    }

    @Test
    void match() throws IOException {

        //1.查询年龄为17的同学
        SearchResponse<SearchStudent> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.match(m -> m
                        .field("age")
                        .query(query -> query.longValue(17))
                )))), SearchStudent.class
        );
        Assert.isTrue(1 == search.hits().hits().size());
        Assert.isTrue(17 == search.hits().hits().get(0).source().getAge());

        //2.查询描述中包含性感的同学
        search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.match(m -> m
                        .field("des")
                        .query(query -> query.stringValue("性感"))
                )), SearchStudent.class
        );
        Assert.isTrue(2 == search.hits().hits().size());
        Assert.isTrue(search.hits().hits().get(0).source().getDes().contains("性感"));
        Assert.isTrue(search.hits().hits().get(1).source().getDes().contains("性感"));

        //3.查询描述中包含性感小姐姐的同学
        search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.match(m -> m
                        .field("des")
                        .query(query -> query.stringValue("性感小姐姐"))
                        .operator(Operator.And)
                )), SearchStudent.class
        );
        Assert.isTrue(2 == search.hits().hits().size());
        Assert.isTrue(
                search.hits().hits().get(0).source().getDes().contains("性感") ||
                        search.hits().hits().get(0).source().getDes().contains("小姐") ||
                        search.hits().hits().get(0).source().getDes().contains("姐姐")
        );
        Assert.isTrue(
                search.hits().hits().get(1).source().getDes().contains("性感") ||
                        search.hits().hits().get(1).source().getDes().contains("小姐") ||
                        search.hits().hits().get(1).source().getDes().contains("姐姐")
        );

        //4.查询描述中包含性感小姐姐的同学,匹配百分比为100%
        search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.match(m -> m
                        .field("des")
                        .query(query -> query.stringValue("性感小姐姐"))
                        .minimumShouldMatch("100%")
                )), SearchStudent.class
        );
        Assert.isTrue(2 == search.hits().hits().size());
        Assert.isTrue(
                search.hits().hits().get(0).source().getDes().contains("性感") ||
                        search.hits().hits().get(0).source().getDes().contains("小姐") ||
                        search.hits().hits().get(0).source().getDes().contains("姐姐")
        );
        Assert.isTrue(
                search.hits().hits().get(1).source().getDes().contains("性感") ||
                        search.hits().hits().get(1).source().getDes().contains("小姐") ||
                        search.hits().hits().get(1).source().getDes().contains("姐姐")
        );
    }

    @Test
    void multiMatch() throws IOException {

        //1.查询描述中包含性感或标签中包含性感的同学
        SearchResponse<SearchStudent> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.multiMatch(m -> m
                        .query("性感")
                        .fields("des", "tags")
                )), SearchStudent.class
        );
        Assert.isTrue(4 == search.hits().hits().size());
        Assert.isTrue(
                search.hits().hits().get(0).source().getDes().contains("性感") ||
                        JSONUtil.toJsonStr(search.hits().hits().get(0).source().getTags()).contains("性感")
        );
        Assert.isTrue(
                search.hits().hits().get(1).source().getDes().contains("性感") ||
                        JSONUtil.toJsonStr(search.hits().hits().get(1).source().getTags()).contains("性感")
        );
        Assert.isTrue(
                search.hits().hits().get(2).source().getDes().contains("性感") ||
                        JSONUtil.toJsonStr(search.hits().hits().get(2).source().getTags()).contains("性感")
        );
        Assert.isTrue(
                search.hits().hits().get(3).source().getDes().contains("性感") ||
                        JSONUtil.toJsonStr(search.hits().hits().get(3).source().getTags()).contains("性感")
        );
    }

    @Test
    void matchPhrase() throws IOException {

        //1.查询描述中包含好看的小姐姐的同学
        SearchResponse<SearchStudent> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.matchPhrase(m -> m
                        .field("des")
                        .query("好看的小姐姐")
                )), SearchStudent.class
        );
        Assert.isTrue(2 == search.hits().hits().size());
        Assert.isTrue(search.hits().hits().get(0).source().getDes().contains("好看的小姐姐"));
        Assert.isTrue(search.hits().hits().get(1).source().getDes().contains("好看的小姐姐"));

        //2.查询描述中包含好看的小姐姐的同学,步长为20
        search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.matchPhrase(m -> m
                        .field("des")
                        .query("好看的小姐姐")
                        .slop(20)
                )), SearchStudent.class
        );
        Assert.isTrue(3 == search.hits().hits().size());
    }

    @Test
    void prefix() throws IOException {

        //1.查询所有名称为王的同学
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.constantScore(c -> c.filter(f -> f.prefix(p -> p
                        .field("name")
                        .value("王")
                )))), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(3 == search.hits().hits().size());
        Assert.isTrue(search.hits().hits().get(0).source().getName().startsWith("王"));
        Assert.isTrue(search.hits().hits().get(1).source().getName().startsWith("王"));
        Assert.isTrue(search.hits().hits().get(2).source().getName().startsWith("王"));
    }

    @Test
    void wildcard() throws IOException {

        //1.查询所有名称为王的同学
        SearchResponse<SearchStudent> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.wildcard(w -> w
                        .field("des")
                        .value("好看")
                )), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(3 == search.hits().hits().size());
        Assert.isTrue(search.hits().hits().get(0).source().getDes().contains("好看"));
        Assert.isTrue(search.hits().hits().get(1).source().getDes().contains("好看"));
        Assert.isTrue(search.hits().hits().get(2).source().getDes().contains("好看"));
    }

    @Test
    void must() throws IOException {

        //1.查询姓氏为王并且年龄为18的同学
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.bool(b -> b
                        .must(m -> m.prefix(p -> p
                                .field("name")
                                .value("王")
                        ))
                        .must(m -> m.term(t -> t
                                .field("age")
                                .value(FieldValue.of(18))
                        ))
                )), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(1 == search.hits().hits().size());
        Assert.isTrue(search.hits().hits().get(0).source().getName().startsWith("王"));
        Assert.isTrue(18 == search.hits().hits().get(0).source().getAge());
    }

    @Test
    void mustNot() throws IOException {

        //1.查询姓氏不为王并且年龄不为18的同学
        SearchResponse<SearchStudent> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.bool(b -> b
                        .mustNot(m -> m.prefix(p -> p
                                .field("name")
                                .value("王")
                        ))
                        .mustNot(m -> m.term(t -> t
                                .field("age")
                                .value(FieldValue.of(18))
                        ))
                )), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(3 == search.hits().hits().size());
        Assert.isTrue(!search.hits().hits().get(0).source().getName().startsWith("王"));
        Assert.isTrue(search.hits().hits().get(0).source().getAge() != 18);
        Assert.isTrue(!search.hits().hits().get(1).source().getName().startsWith("王"));
        Assert.isTrue(search.hits().hits().get(1).source().getAge() != 18);
        Assert.isTrue(!search.hits().hits().get(2).source().getName().startsWith("王"));
        Assert.isTrue(search.hits().hits().get(2).source().getAge() != 18);
    }

    @Test
    void shouldWithoutMust() throws IOException {

        //1.查询年龄为18或标签中包含性感的同学
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.bool(b -> b
                        .should(should -> should.term(t -> t
                                .field("age")
                                .value(FieldValue.of(18))
                        ))
                        .should(should -> should.term(t -> t
                                .field("tags")
                                .value(FieldValue.of("性感"))
                        ))
                )), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(4 == search.hits().hits().size());
        Assert.isTrue(
                search.hits().hits().get(0).source().getAge() == 18 ||
                        search.hits().hits().get(0).source().getTags().contains("性感")
        );
        Assert.isTrue(
                search.hits().hits().get(1).source().getAge() == 18 ||
                        search.hits().hits().get(1).source().getTags().contains("性感")
        );
        Assert.isTrue(
                search.hits().hits().get(2).source().getAge() == 18 ||
                        search.hits().hits().get(2).source().getTags().contains("性感")
        );
        Assert.isTrue(
                search.hits().hits().get(3).source().getAge() == 18 ||
                        search.hits().hits().get(3).source().getTags().contains("性感")
        );
    }

    @Test
    void shouldWithMust() throws IOException {

        //1.查询描述匹配好看的小姐姐短语且年龄为18的同学，同时标签中包含风骚的同学优先级高
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.bool(b -> b
                        .must(m -> m.match(match -> match
                                .field("des")
                                .query(query -> query.stringValue("好看的小姐姐"))
                        ))
                        .must(m -> m.term(t -> t
                                .field("age")
                                .value(FieldValue.of(18))
                        ))
                        .should(should -> should.term(t -> t
                                .field("tags")
                                .value(FieldValue.of("风骚"))
                        ))
                )), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(2 == search.hits().hits().size());
        Assert.isTrue(search.hits().hits().get(0).source().getTags().contains("风骚"));
        Assert.isTrue(!search.hits().hits().get(1).source().getTags().contains("风骚"));
    }

    @Test
    void mustAndShould() throws IOException {

        //1.查询姓氏为王，并且标签包含性感或可爱的同学
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.bool(b -> b
                        .filter(f -> f.prefix(p -> p
                                .field("name")
                                .value("王")
                        ))
                        .filter(f -> f.bool(fb -> fb.should(fbs -> fbs.terms(t -> t
                                .field("tags")
                                .terms(tt -> tt.value(CollUtil.newArrayList(
                                        FieldValue.of("性感"),
                                        FieldValue.of("可爱")
                                )))
                        ))))
                )), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(2 == search.hits().hits().size());
        Assert.isTrue(search.hits().hits().get(0).source().getName().startsWith("王"));
        Assert.isTrue(
                search.hits().hits().get(0).source().getTags().contains("性感") ||
                        search.hits().hits().get(0).source().getTags().contains("可爱")
        );
        Assert.isTrue(search.hits().hits().get(1).source().getName().startsWith("王"));
        Assert.isTrue(
                search.hits().hits().get(1).source().getTags().contains("性感") ||
                        search.hits().hits().get(1).source().getTags().contains("可爱")
        );

        //3.查询年龄为18或标签中包含长腿和性感的同学
        search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.bool(b -> b
                        .should(should -> should.term(t -> t
                                .field("age")
                                .value(FieldValue.of(18))
                        ))
                        .should(should -> should.bool(sb -> sb.must(sbm -> sbm.terms(t -> t
                                .field("tags")
                                .terms(terms -> terms.value(CollUtil.newArrayList(
                                        FieldValue.of("长腿"),
                                        FieldValue.of("性感")
                                )))
                        ))))
                )), SearchStudent.class
        );

        //4.校验
        Assert.isTrue(4 == search.hits().hits().size());
        Assert.isTrue(
                18 == search.hits().hits().get(0).source().getAge() || (
                        search.hits().hits().get(0).source().getTags().contains("长腿") &&
                                search.hits().hits().get(0).source().getTags().contains("性感")
                ));
        Assert.isTrue(
                18 == search.hits().hits().get(1).source().getAge() || (
                        search.hits().hits().get(1).source().getTags().contains("长腿") &&
                                search.hits().hits().get(1).source().getTags().contains("性感")
                ));
        Assert.isTrue(
                18 == search.hits().hits().get(2).source().getAge() || (
                        search.hits().hits().get(2).source().getTags().contains("长腿") &&
                                search.hits().hits().get(2).source().getTags().contains("性感")
                ));
        Assert.isTrue(
                18 == search.hits().hits().get(3).source().getAge() || (
                        search.hits().hits().get(3).source().getTags().contains("长腿") &&
                                search.hits().hits().get(3).source().getTags().contains("性感")
                ));
    }

    @Test
    void matchAll() throws IOException {

        //1.全量查询
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                .index(INDEX_NAME)
                .query(q -> q.matchAll(m -> m)), SearchStudent.class
        );

        //2.校验
        Assert.isTrue(7 == search.hits().hits().size());
    }

    @Test
    void sort() throws IOException {

        //1.按照年龄排序
        SearchResponse<SearchStudent> search = primaryClient.search(s -> s
                        .index(INDEX_NAME)
                        .sort(sort -> sort.field(f -> f.field("age").order(SortOrder.Asc)))
                        .sort(sort -> sort.field(f -> f.field("grade").order(SortOrder.Asc))),
                SearchStudent.class
        );

        //2.验证
        Assert.isTrue(7 == search.hits().hits().size());
        Assert.isTrue(17 == search.hits().hits().get(0).source().getAge());
        Assert.isTrue(28 == search.hits().hits().get(6).source().getAge());
        Assert.isTrue(16.7 == search.hits().hits().get(1).source().getGrade());
        Assert.isTrue(22.7 == search.hits().hits().get(2).source().getGrade());
    }

    @Test
    void fromSize() throws IOException {

        //1.每页3个，查询第3页数据
        SearchResponse<SearchStudent> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .from(6)
                .size(3), SearchStudent.class
        );

        //2.验证
        Assert.isTrue(1 == search.hits().hits().size());
    }

    @Test
    void scroll() throws IOException {

        //1.定义结果集
        List<SearchStudent> students = CollUtil.newArrayList();

        //2.查询快照数据
        SearchResponse<SearchStudent> search = secondaryClient.search(s -> s
                .index(INDEX_NAME)
                .scroll(scroll -> scroll.time("10m"))
                .sort(sort -> sort.field(f -> f
                        .field("grade")
                        .order(SortOrder.Asc)
                ))
                .size(2), SearchStudent.class
        );

        //3.初始数据加入结果集
        students.addAll(CollStreamUtil.toList(search.hits().hits(), Hit::source));

        //4.定义count值
        int count = Integer.MIN_VALUE;
        String scrollId = search.scrollId();

        //5.循环从快照获取值
        while (count != 0) {

            //6.查询快照数据
            search = secondaryClient.scroll(s -> s
                    .scrollId(scrollId)
                    .scroll(scroll -> scroll.time("10m")), SearchStudent.class
            );

            //7.重新复制count
            count = search.hits().hits().size();

            //8.数据继续加入结果集
            students.addAll(CollStreamUtil.toList(search.hits().hits(), Hit::source));
        }

        //9.校验
        Assert.isTrue(7 == students.size());
    }
}
