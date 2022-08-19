package com.ftc.elasticsearchtest.config;

import cn.hutool.core.collection.CollStreamUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.ReindexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SpringBootTest
class ElasticSearchIndexTest {

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

    @BeforeEach
    void removeApplicationIndex() throws IOException {

        //1.查询所有索引
        IndicesResponse catIndies = primaryClient.cat().indices();
        List<IndicesRecord> indicesRecords = catIndies.valueBody();

        //2.获取所有自定义索引名称
        List<String> applicationIndexNames = indicesRecords.stream()
                .map(IndicesRecord::index)
                .filter(index -> !index.startsWith(StrUtil.DOT))
                .collect(Collectors.toList());

        //3.判空返回
        if (CollUtil.isEmpty(applicationIndexNames)) {
            return;
        }

        //4.删除
        ElasticsearchIndicesClient indicesClient = primaryClient.indices();
        DeleteIndexResponse delete = indicesClient.delete(i -> i.index(applicationIndexNames));
        Assert.isTrue(delete.acknowledged());
    }

    @Test
    void testCreateIndex() throws IOException {

        //1.创建无参索引
        CreateIndexResponse createIndexResponse = secondaryClient
                .indices()
                .create(i -> i.index(INDEX_NAME));
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
    }

    @Test
    void testAlias() throws IOException {

        //1.创建无参索引
        CreateIndexResponse createIndexResponse = secondaryClient
                .indices()
                .create(i -> i.index(INDEX_NAME + "_1"));
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
        createIndexResponse = secondaryClient
                .indices()
                .create(i -> i.index(INDEX_NAME + "_2"));
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));

        //2.多个索引添加一个别名
        PutAliasResponse putAliasResponse = secondaryClient
                .indices()
                .putAlias(a -> a
                        .index(INDEX_NAME + "_1")
                        .name(INDEX_NAME)
                );
        Assert.isTrue(putAliasResponse.acknowledged());

        //3.更新索引别名
        UpdateAliasesResponse updateAliasesResponse = secondaryClient
                .indices()
                .updateAliases(i -> i.actions(
                        Action.of(a -> a.remove(
                                r -> r.indices(INDEX_NAME + "_1").aliases(INDEX_NAME)
                        )),
                        Action.of(a -> a.add(
                                r -> r.indices(INDEX_NAME + "_2").aliases(INDEX_NAME)
                        ))
                ));
        Assert.isTrue(updateAliasesResponse.acknowledged());

        //4.删除别名
        DeleteAliasResponse deleteAliasResponse = secondaryClient
                .indices()
                .deleteAlias(i -> i
                        .index(INDEX_NAME + "_1", INDEX_NAME + "_2")
                        .name(CollUtil.newArrayList(INDEX_NAME))
                );
        Assert.isTrue(deleteAliasResponse.acknowledged());

        //5.查询别名
        GetAliasResponse alias = secondaryClient
                .indices()
                .getAlias(i -> i.index(INDEX_NAME + "_1", INDEX_NAME + "_2"));

        //6.校验查询结果
        Set<String> indexNames = alias.result().keySet();
        Assert.isTrue(2 == indexNames.size());
        indexNames.forEach(indexName -> {
            Set<String> aliases = alias.result().get(indexName).aliases().keySet();
            Assert.isTrue(0 == aliases.size());
        });
    }

    @Test
    void testCreateIndexWithAlias() throws IOException {

        //1.创建单个别名索引
        Alias alias = Alias.of(a -> a);
        CreateIndexResponse createIndexResponse = secondaryClient
                .indices()
                .create(i -> i
                        .index(INDEX_NAME + "_1")
                        .aliases(INDEX_NAME, alias)
                );
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));

        //2.创建多个别名索引
        Map<String, Alias> aliasMap = MapUtil.newHashMap(2);
        aliasMap.put(INDEX_NAME, Alias.of(a -> a));
        aliasMap.put(INDEX_NAME + "_bak", Alias.of(a -> a));
        createIndexResponse = secondaryClient
                .indices()
                .create(i -> i
                        .index(INDEX_NAME + "_2")
                        .aliases(aliasMap)
                );
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));

        //3.查询索引别名
        GetAliasResponse aliasResponse = secondaryClient
                .indices()
                .getAlias(i -> i.index(INDEX_NAME + "_1", INDEX_NAME + "_2"));

        //4.校验索引别名
        Map<String, IndexAliases> result = aliasResponse.result();
        Set<String> aliasNames = result.get(INDEX_NAME + "_1").aliases().keySet();
        Assert.isTrue("[\"test_index\"]".equals(JSONUtil.toJsonStr(aliasNames)));
        aliasNames = result.get(INDEX_NAME + "_2").aliases().keySet();
        Assert.isTrue("[\"test_index_bak\",\"test_index\"]".equals(JSONUtil.toJsonStr(aliasNames)));
    }

    @Test
    void testCreateIndexWithMapping() throws IOException {

        //1.创建索引
        CreateIndexResponse createIndexResponse = primaryClient
                .indices()
                .create(i -> i
                        .index(INDEX_NAME)
                        .mappings(m -> m
                                .properties("name", p -> p.text(t -> t.analyzer("ik_max_word")))
                                .properties("age", p -> p.integer(in -> in))
                                .properties("tags", p -> p.keyword(k -> k))
                        )
                );
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
    }

    @Test
    void testCreateIndexWithSetting() throws IOException {

        //1.创建索引
        CreateIndexResponse createIndexResponse = primaryClient
                .indices()
                .create(i -> i
                        .index(INDEX_NAME)
                        .settings(s -> s
                                .refreshInterval(r -> r.time("1s"))
                                .numberOfShards("3")
                                .numberOfReplicas("1")
                        )
                );
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
    }

    @Test
    void testCreateIndexWithAllArgs() throws IOException {

        //1.创建索引
        CreateIndexResponse createIndexResponse = secondaryClient
                .indices()
                .create(i -> i
                        .index(INDEX_NAME + "_1")
                        .aliases(INDEX_NAME, a -> a)
                        .mappings(m -> m
                                .properties("name", p -> p.text(t -> t.analyzer("ik_max_word")))
                                .properties("age", p -> p.integer(in -> in))
                                .properties("tags", p -> p.keyword(k -> k))
                        )
                        .settings(s -> s
                                .refreshInterval(r -> r.time("1s"))
                                .numberOfShards("3")
                                .numberOfReplicas("1")
                        )
                );
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));
    }

    @Test
    void testGetIndexes() throws IOException {

        //1.创建测试索引
        CreateIndexResponse response = primaryClient.indices().create(i -> i.index(INDEX_NAME));
        Assert.isTrue(Boolean.TRUE.equals(response.acknowledged()));

        //2.查询全部索引
        IndicesResponse indices = primaryClient.cat().indices();
        List<String> indexNames = CollStreamUtil.toList(indices.valueBody(), IndicesRecord::index);
        Assert.isTrue(indexNames.contains(INDEX_NAME));

        //3.查询单个索引
        GetIndexResponse getIndexResponse = primaryClient.indices().get(i -> i.index(INDEX_NAME));
        Map<String, IndexState> result = getIndexResponse.result();
        IndexState indexState = result.get(INDEX_NAME);
        Assert.isTrue(ObjectUtil.isNotNull(indexState));

        //4.获取索引单个属性
        Map<String, Alias> aliases = indexState.aliases();
        TypeMapping mappings = indexState.mappings();
        IndexSettings settings = indexState.settings();
        Assert.isTrue(ObjectUtil.isNotNull(aliases));
        Assert.isTrue(ObjectUtil.isNotNull(mappings));
        Assert.isTrue(ObjectUtil.isNotNull(settings));
    }

    @Test
    void testExistIndex() throws IOException {

        //1.验证是否存在
        BooleanResponse exists = primaryClient.indices().exists(e -> e.index(INDEX_NAME));

        //2.校验结果
        Assert.isFalse(exists.value());
    }

    @Test
    void testDeleteIndex() throws IOException {

        //1.创建索引
        CreateIndexResponse createIndexResponse = primaryClient
                .indices()
                .create(i -> i.index(INDEX_NAME));
        Assert.isTrue(Boolean.TRUE.equals(createIndexResponse.acknowledged()));

        //2.验证是否存在
        BooleanResponse exists = primaryClient.indices().exists(e -> e.index(INDEX_NAME));
        Assert.isTrue(exists.value());

        //3.删除索引
        DeleteIndexResponse delete = primaryClient
                .indices()
                .delete(d -> d.index(INDEX_NAME));
        Assert.isTrue(delete.acknowledged());

        //4.验证是否存在
        exists = primaryClient.indices().exists(e -> e.index(INDEX_NAME));
        Assert.isFalse(exists.value());
    }

    @Test
    void testUpdateIndex() throws IOException, InterruptedException {

        //1.创建老索引
        CreateIndexResponse response = primaryClient
                .indices()
                .create(i -> i
                        .index(INDEX_NAME + "_1")
                        .aliases(INDEX_NAME, a -> a)
                        .settings(s -> s
                                .refreshInterval(r -> r.time("1s"))
                        )
                );
        Assert.isTrue(Boolean.TRUE.equals(response.acknowledged()));

        //2.创建新索引
        response = primaryClient
                .indices()
                .create(i -> i
                        .index(INDEX_NAME + "_2")
                        .aliases(INDEX_NAME, a -> a)
                        .settings(s -> s
                                .refreshInterval(r -> r.time("1s"))
                                .numberOfShards("3")
                                .numberOfReplicas("1")
                        )
                );
        Assert.isTrue(Boolean.TRUE.equals(response.acknowledged()));

        //3.老索引存入一条文档
        JSONObject entries = new JSONObject();
        entries.set("age", 1);
        IndexResponse index = primaryClient.index(i -> i
                .index(INDEX_NAME + "_1")
                .document(entries)
        );
        String id = index.id();
        Assert.isTrue(StrUtil.isNotBlank(id));

        //4.线程睡一会，给分片一个refresh的时间
        TimeUnit.SECONDS.sleep(1);

        //5.老索引数据迁移到新索引
        ReindexResponse reindex = primaryClient.reindex(r -> r
                .source(s -> s.index(INDEX_NAME + "_1"))
                .dest(d -> d.index(INDEX_NAME + "_2"))
        );
        Assert.isTrue(1 == reindex.batches());

        //6.删除老索引
        DeleteIndexResponse delete = primaryClient.indices().delete(d -> d.index(INDEX_NAME + "_1"));
        Assert.isTrue(delete.acknowledged());

        //7.验证老索引不存在
        BooleanResponse exists = primaryClient.indices().exists(e -> e.index(INDEX_NAME + "_1"));
        Assert.isFalse(exists.value());

        //8.线程再睡一会，给新索引一个refresh的时间
        TimeUnit.SECONDS.sleep(1);

        //9.查询数据在新索引
        SearchResponse<JSONObject> search = primaryClient.search(s -> s
                        .index(INDEX_NAME + "_2")
                , JSONObject.class);
        String resultId = search.hits().hits().get(0).id();
        Assert.isTrue(id.equals(resultId));
    }
}