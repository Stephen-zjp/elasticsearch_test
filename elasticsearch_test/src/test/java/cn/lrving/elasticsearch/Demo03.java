package cn.lrving.elasticsearch;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class Demo03 {

    /** 创建索引库(集群环境) */
    @Test
    public void test1() throws Exception{
        // 1. 创建Settings配置信息对象(主要配置集群名称)
        // 参数一: 集群key (固定不变)
        // 参数二：集群环境名称,默认的ES的环境集群名称为 "elasticsearch"
        Settings settings = Settings.builder()
                .put("cluster.name", "cluster-es").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        // 2.1 添加传输地址对象(集群环境多个)
        // 参数一：主机
        // 参数二：端口
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

        // 3. 创建索引库(index)
        // 获取创建索引库请求构建对象
        CreateIndexRequestBuilder cirb = transportClient.admin().indices()
                .prepareCreate("blog1");
        // 3.1 创建Map集合封装分片与副本设置信息
        Map<String, Integer> source = new HashMap<String, Integer>();
        // 3.2 设置分片数量
        source.put("number_of_shards", 3);
        // 3.3 设置副本数量
        source.put("number_of_replicas", 1);
        // 3.4 设置map集合，执行请求
        cirb.setSettings(source).get();

        // 4. 释放资源
        transportClient.close();
    }

    /** 添加文档 (集群环境) */
    @Test
    public void test2() throws Exception{
        // 1. 创建Settings配置信息对象(主要配置集群名称)
        Settings settings = Settings.builder()
                .put("cluster.name", "cluster-es").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

        // 3. 创建内容构建对象
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", 1)
                .field("title", "elasticsearch搜索服务")
                .field("content","ElasticSearch是一个基于Lucene的搜索服务器。它提供了一个分布式多用户能力的全文搜索引擎。")
                .endObject();

        // 4. 执行索引库、类型、文档
        transportClient.prepareIndex("blog1","article", "1")
                .setSource(builder).get(); // 执行请求
        // 5. 释放资源
        transportClient.close();
    }


    /** 匹配全部 (集群环境) */
    @Test
    public void test3()throws Exception{

        // 1. 创建Settings配置信息对象(主要配置集群名称)
        Settings settings = Settings.builder()
                .put("cluster.name", "cluster-es").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9301));
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9302));
        transportClient.addTransportAddress(new
                TransportAddress(InetAddress.getByName("127.0.0.1"), 9303));

        // 3. 创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog1").setTypes("article");
        // 3.1 设置查询条件(匹配全部)
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        // 4. 执行请求，得到搜索响应对象
        SearchResponse searchResponse = searchRequestBuilder.get();

        // 5. 获取搜索结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("总命中数：" + hits.totalHits);

        // 6. 迭代搜索结果
        for (SearchHit hit : hits) {
            System.out.println("JSON字符串：" + hit.getSourceAsString());
            System.out.println("id: " + hit.getSourceAsMap().get("id"));
            System.out.println("title: " + hit.getSourceAsMap().get("title"));
            System.out.println("content: " + hit.getSourceAsMap().get("content"));
        }
        // 7. 释放资源
        transportClient.close();
    }
}