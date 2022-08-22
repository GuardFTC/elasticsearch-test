package com.ftc.elasticsearchtest.config;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.ftc.elasticsearchtest.entity.Student;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-22 11:11:22
 * @describe: ElasticSearch文档API单元测试
 */
@SpringBootTest
public class ElasticsearchDocTest {

    @Resource
    @Qualifier("primaryElasticsearchClient")
    private ElasticsearchClient primaryClient;

    @Resource
    @Qualifier("secondaryElasticsearchClient")
    private ElasticsearchClient secondaryClient;

    /**
     * 默认文档ID
     */
    private static final String DEFAULT_ID = "66";

    /**
     * 测试Mock索引名称常量
     */
    private static final String INDEX_NAME = "test_index";

    @BeforeEach
    void createIndexAndFlushAll() throws IOException {

        //1.判定索引是否存在
        boolean exist = primaryClient
                .indices()
                .exists(e -> e.index(INDEX_NAME + "_1"))
                .value();

        //2.索引不存在创建索引
        if (!exist) {
            CreateIndexResponse createIndexResponse = primaryClient
                    .indices()
                    .create(i -> i
                            .index(INDEX_NAME + "_1")
                            .aliases(INDEX_NAME, a -> a)
                            .mappings(m -> m
                                    .properties("name", p -> p.text(t -> t.analyzer("ik_max_word")))
                                    .properties("age", p -> p.integer(in -> in))
                            )
                            .settings(s -> s
                                    .refreshInterval(t -> t.time("1s"))
                                    .numberOfShards("3")
                                    .numberOfReplicas("1")
                            )
                    );
            Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
        }

        //3.清空索引数据
        DeleteByQueryResponse delete = primaryClient.deleteByQuery(d -> d
                .index(INDEX_NAME)
                .query(q -> q.matchAll(m -> m))
                .refresh(true)
        );
        Assert.isTrue(ObjectUtil.isNotNull(delete.deleted()));

        //4.创建最新数据
        Student student = new Student();
        student.setId(66);
        student.setName("宋可心");
        student.setAge(18);

        //5.存入最新数据
        IndexResponse index = primaryClient.index(i -> i
                .index(INDEX_NAME)
                .document(student)
                .id(DEFAULT_ID)
                .refresh(Refresh.True)
        );
        Assert.isTrue("created".equals(index.result().jsonValue().toLowerCase(Locale.ROOT)));
    }

    @Test
    void createDocSingle() throws IOException {

        //1.创建学生对象
        Student student = new Student();
        student.setId(1);
        student.setName("冯铁城");
        student.setAge(11);

        //2.创建单个文档不指定ID
        IndexResponse indexResponse = primaryClient.index(i -> i
                .index(INDEX_NAME)
                .document(student)
        );
        Result result = indexResponse.result();
        Assert.isTrue("created".equals(result.jsonValue().toLowerCase(Locale.ROOT)));

        //3.创建单个文档指定ID
        indexResponse = primaryClient.index(i -> i
                .index(INDEX_NAME)
                .document(student)
                .id("1")
        );
        result = indexResponse.result();
        Assert.isTrue("created".equals(result.jsonValue().toLowerCase(Locale.ROOT)));

        //4.创建单个文档进行ID唯一性控制
        String errorMessage = null;
        try {
            primaryClient.index(i -> i
                    .index(INDEX_NAME)
                    .document(student)
                    .id("1")
                    .opType(OpType.Create)
            );
        } catch (Exception e) {
            errorMessage = e.getMessage();
        }
        Assert.isTrue(ObjectUtil.isNotNull(errorMessage));
        Assert.isTrue(errorMessage.contains("version_conflict_engine_exception"));
    }

    @Test
    void createDocBulk() throws IOException {

        //1.创建学生对象
        Student student = new Student();
        student.setId(1);
        student.setName("张钰玲");
        student.setAge(11);

        //2.批量保存数据不带ID
        BulkResponse bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.create(c -> c.document(student)))
                .operations(o -> o.create(c -> c.document(student)))
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());

        //3.批量保存数据带ID
        bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.create(c -> c.document(student).id("1")))
                .operations(o -> o.create(c -> c.document(student).id("2")))
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());
    }

    @Test
    void getDocSingle() throws IOException {

        //1.创建学生对象
        Student student = new Student();
        student.setId(1);
        student.setName("冯铁城");
        student.setAge(18);
        Student studentBak = new Student();
        student.setId(2);
        student.setName("张钰玲");
        student.setAge(16);

        //2.批量保存数据
        BulkResponse bulk = secondaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.create(c -> c.document(student).id("1")))
                .operations(o -> o.create(c -> c.document(studentBak).id("2")))
                .refresh(Refresh.True)
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());

        //3.根据ID查询单个数据
        Student source = secondaryClient.get(g -> g
                .index(INDEX_NAME)
                .id("1"), Student.class
        ).source();
        Assert.isTrue(JSONUtil.toJsonStr(student).equals(JSONUtil.toJsonStr(source)));

        //4.按照ID批量查询
        List<MultiGetResponseItem<Student>> docs = secondaryClient.mget(m -> m
                .index(INDEX_NAME)
                .ids("1", "2"), Student.class
        ).docs();
        Assert.isTrue(2 == docs.size());
        Student source1 = docs.get(0).result().source();
        Assert.isTrue(JSONUtil.toJsonStr(student).equals(JSONUtil.toJsonStr(source1)));
        Student source2 = docs.get(1).result().source();
        Assert.isTrue(JSONUtil.toJsonStr(studentBak).equals(JSONUtil.toJsonStr(source2)));
    }

    @Test
    void existDoc() throws IOException {

        //1.验证数据存在
        BooleanResponse exists = secondaryClient.exists(e -> e
                .index(INDEX_NAME)
                .id(DEFAULT_ID)
        );
        Assert.isTrue(exists.value());

        //2.验证数据不存在
        exists = secondaryClient.exists(e -> e
                .index(INDEX_NAME)
                .id(DEFAULT_ID + "1")
        );
        Assert.isFalse(exists.value());
    }

    @Test
    void updateDocSingle() throws IOException {

        //1.查询默认数据
        Student student = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id(DEFAULT_ID), Student.class
        ).source();
        Assert.isTrue(ObjectUtil.isNotNull(student));

        //2.全量更新单个数据（立即refresh，为了单元测试方便）
        student.setAge(100);
        IndexResponse index = primaryClient.index(i -> i
                .index(INDEX_NAME)
                .document(student)
                .id(DEFAULT_ID)
                .refresh(Refresh.True)
        );
        Assert.isTrue("updated".equals(index.result().jsonValue().toLowerCase(Locale.ROOT)));

        //3.查询校验
        Integer age = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id(DEFAULT_ID), Student.class
        ).source().getAge();
        Assert.isTrue(100 == age);

        //4.全量更新单个数据并进行版本控制（立即refresh，为了单元测试方便）
        student.setAge(11);
        long primaryTerm = index.primaryTerm();
        long seqNo = index.seqNo();
        index = primaryClient.index(i -> i
                .index(INDEX_NAME)
                .document(student)
                .id(DEFAULT_ID)
                .ifPrimaryTerm(primaryTerm)
                .ifSeqNo(seqNo)
                .refresh(Refresh.True)
        );
        Assert.isTrue("updated".equals(index.result().jsonValue().toLowerCase(Locale.ROOT)));

        //5.查询校验
        age = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id(DEFAULT_ID), Student.class
        ).source().getAge();
        Assert.isTrue(11 == age);

        //6.全量更新单个数据并进行版本控制（立即refresh，为了单元测试方便）
        String errorMessage = null;
        try {
            primaryClient.index(i -> i
                    .index(INDEX_NAME)
                    .document(student)
                    .id(INDEX_NAME)
                    .ifPrimaryTerm((long) 1)
                    .ifSeqNo((long) 2)
                    .refresh(Refresh.True)
            );
        } catch (Exception e) {
            errorMessage = e.getMessage();
        }
        Assert.isTrue(StrUtil.isNotBlank(errorMessage));
        Assert.isTrue(errorMessage.contains("version_conflict_engine_exception"));

        //7.更新文档部分属性
        Student studentBak = new Student();
        studentBak.setAge(30);
        UpdateResponse<Student> update = primaryClient.update(u -> u
                .index(INDEX_NAME)
                .id(DEFAULT_ID)
                .doc(studentBak)
                .refresh(Refresh.True), Student.class
        );
        Assert.isTrue("updated".equals(update.result().jsonValue().toLowerCase(Locale.ROOT)));

        //8.查询校验
        Student source = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id(DEFAULT_ID), Student.class
        ).source();
        Assert.isTrue(ObjectUtil.isNotNull(source));
        Assert.isTrue(studentBak.getAge() == source.getAge());
        Assert.isTrue(student.getName().equals(source.getName()));
    }

    @Test
    void updateDocBulk() throws IOException {

        //1.准备测试数据
        Student student = new Student();
        student.setName("冯铁城");
        student.setAge(18);
        Student studentBak = new Student();
        studentBak.setName("张钰玲");
        studentBak.setAge(16);

        //2.存入测试数据
        BulkResponse bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.create(c -> c.document(student).id("1")))
                .operations(o -> o.create(c -> c.document(studentBak).id("2")))
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());

        //3.批量更新部分数据
        Student bulkStudent = new Student();
        bulkStudent.setAge(111);
        bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.update(u -> u.action(a -> a.doc(bulkStudent)).id("1")))
                .operations(o -> o.update(u -> u.action(a -> a.doc(bulkStudent)).id("2")))
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());

        //4.查询校验
        Student source = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id("1"), Student.class
        ).source();
        Assert.isTrue("{\"name\":\"冯铁城\",\"age\":111}".equals(JSONUtil.toJsonStr(source)));
        source = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id("2"), Student.class
        ).source();
        Assert.isTrue("{\"name\":\"张钰玲\",\"age\":111}".equals(JSONUtil.toJsonStr(source)));

        //5.批量更新全量数据
        Student bulkStudent2 = new Student();
        bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.index(i -> i.document(bulkStudent2).id("1")))
                .operations(o -> o.index(i -> i.document(bulkStudent2).id("2")))
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());

        //6.查询验证
        source = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id("1"), Student.class
        ).source();
        Assert.isTrue("{}".equals(JSONUtil.toJsonStr(source)));
        source = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id("2"), Student.class
        ).source();
        Assert.isTrue("{}".equals(JSONUtil.toJsonStr(source)));
    }

    @Test
    void updateDocQuery() throws IOException {

        //1.准备测试数据
        Student student = new Student();
        student.setName("冯铁城");
        student.setAge(18);
        Student studentBak = new Student();
        studentBak.setName("张钰玲");
        studentBak.setAge(16);

        //2.存入测试数据
        BulkResponse bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.create(c -> c.document(student).id("1")))
                .operations(o -> o.create(c -> c.document(studentBak).id("2")))
                .refresh(Refresh.True)
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());

        //3.更新名称为冯铁城的数据，将其年龄改为100
        UpdateByQueryResponse updateByQueryResponse = primaryClient.updateByQuery(u -> u
                .index(INDEX_NAME)
                .query(q -> q.match(m -> m
                        .field("name")
                        .query(query -> query.stringValue("冯铁城"))
                ))
                .script(s -> s.inline(i -> i
                        .source("ctx._source.age=100")
                ))
                .refresh(true)
        );
        Assert.isTrue(1 == updateByQueryResponse.updated());

        //4.查询校验
        Student source = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id("1"), Student.class
        ).source();
        Assert.isTrue(100 == source.getAge());

        //5.更新名称为冯铁城的数据，移除age属性
        updateByQueryResponse = primaryClient.updateByQuery(u -> u
                .index(INDEX_NAME)
                .query(q -> q.match(m -> m
                        .field("name")
                        .query(query -> query.stringValue("冯铁城"))
                ))
                .script(s -> s.inline(i -> i
                        .source("ctx._source.remove('age')")
                ))
                .refresh(true)
        );
        Assert.isTrue(1 == updateByQueryResponse.updated());

        //6.查询校验
        source = primaryClient.get(g -> g
                .index(INDEX_NAME)
                .id("1"), Student.class
        ).source();
        Assert.isNull(source.getAge());
    }

    @Test
    void deleteDocSingle() throws IOException {

        //1.验证默认文档存在
        BooleanResponse exists = secondaryClient.exists(e -> e.index(INDEX_NAME).id(DEFAULT_ID));
        Assert.isTrue(exists.value());

        //2.删除文档
        DeleteResponse delete = secondaryClient.delete(d -> d
                .index(INDEX_NAME)
                .id(DEFAULT_ID)
                .refresh(Refresh.True)
        );
        Assert.isTrue("deleted".equals(delete.result().jsonValue().toLowerCase(Locale.ROOT)));

        //3.再次验证，文档已不存在
        exists = secondaryClient.exists(e -> e.index(INDEX_NAME).id(DEFAULT_ID));
        Assert.isFalse(exists.value());
    }

    @Test
    void deleteDocBulk() throws IOException {

        //1.存入测试数据
        BulkResponse bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.create(c -> c.document(new Student()).id("1")))
                .operations(o -> o.create(c -> c.document(new Student()).id("2")))
                .refresh(Refresh.True)
        );
        Assert.isFalse(bulk.errors());

        //2.验证文档已存在
        BooleanResponse exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("1"));
        Assert.isTrue(exists.value());
        exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("2"));
        Assert.isTrue(exists.value());

        //3.批量删除
        bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.delete(d -> d.id("1")))
                .operations(o -> o.delete(d -> d.id("2")))
                .refresh(Refresh.True)
        );
        Assert.isFalse(bulk.errors());
        Assert.isTrue(2 == bulk.items().size());

        //4.验证已不存在
        exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("1"));
        Assert.isFalse(exists.value());
        exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("2"));
        Assert.isFalse(exists.value());
    }

    @Test
    void deleteDocQuery() throws IOException {

        //1.准备测试数据
        Student student1 = new Student();
        student1.setAge(11);
        Student student2 = new Student();
        student2.setAge(18);

        //2.存入测试数据
        BulkResponse bulk = primaryClient.bulk(b -> b
                .index(INDEX_NAME)
                .operations(o -> o.create(c -> c.document(student1).id("1")))
                .operations(o -> o.create(c -> c.document(student2).id("2")))
                .refresh(Refresh.True)
        );
        Assert.isFalse(bulk.errors());

        //3.校验是否存在
        BooleanResponse exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("1"));
        Assert.isTrue(exists.value());
        exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("2"));
        Assert.isTrue(exists.value());

        //4.删除数据
        DeleteByQueryResponse age = primaryClient.deleteByQuery(d -> d
                .index(INDEX_NAME)
                .query(q -> q.term(t -> t
                        .field("age")
                        .value(v -> v.longValue(11))
                ))
                .refresh(true)
        );
        Assert.isTrue(1 == age.deleted());

        //5.校验是否存在
        exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("1"));
        Assert.isFalse(exists.value());
        exists = primaryClient.exists(e -> e.index(INDEX_NAME).id("2"));
        Assert.isTrue(exists.value());
    }
}
