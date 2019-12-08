package cn.lrving.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

public class Demo02 {
    /** 匹配全部 */
    @Test
    public void test01() throws UnknownHostException {
        //1.创建settings配置信息
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();

        //2.创建ES传输地址对象
        TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY);

        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new TransportAddress(
                InetAddress.getByName("127.0.0.1"), 9300));

        //3.创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog2").setTypes("article");

        //4.设置查询条件
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());

        //5.执行搜索，得到搜索响应对象
        SearchResponse searchResponse = searchRequestBuilder.get();

        //6.获取响应数据
        SearchHits hits = searchResponse.getHits();
        System.out.println("总命中数：" + hits.getTotalHits());

        //7.迭代响应数据
        for (SearchHit hit : hits){
            System.out.println("===============");
            //全部
            System.out.println("json字符串"+hit.getSourceAsString());
            //根据键找值
            System.out.println("id:" + hit.getSourceAsMap().get("id"));
            System.out.println("title:" + hit.getSourceAsMap().get("title"));
            System.out.println("content:" + hit.getSourceAsMap().get("content"));
        }
        //8.释放资源
        transportClient.close();
    }


    /** 字符串查询 */
    @Test
    public void test2()throws Exception{

        // 1. 创建Settings配置信息对象
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new TransportAddress(
                InetAddress.getByName("127.0.0.1"), 9300));

        // 3. 创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog2").setTypes("article");
        // 3.1 设置查询条件(字符串查询)
        searchRequestBuilder.setQuery(QueryBuilders.queryStringQuery("搜索服务"));

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

    /** 词条查询(搜索条件不分词) */
    @Test
    public void test3()throws Exception{

        // 1. 创建Settings配置信息对象
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new TransportAddress(
                InetAddress.getByName("127.0.0.1"), 9300));

        // 3. 创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog2").setTypes("article");
        // 3.1 设置查询条件(词条查询)
        searchRequestBuilder.setQuery(QueryBuilders.termQuery("title","搜索服务"));

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

    /** 根据id主键查询*/
    @Test
    public void test4()throws Exception{

        // 1. 创建Settings配置信息对象
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new TransportAddress(
                InetAddress.getByName("127.0.0.1"), 9300));

        // 3. 创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog2").setTypes("article");
        // 3.1 设置查询条件(多个主键id)
        searchRequestBuilder.setQuery(QueryBuilders.idsQuery().addIds("2","3"));

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

    /** 范围查询 */
    @Test
    public void test5()throws Exception{
        // 1. 创建Settings配置信息对象
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new TransportAddress(
                InetAddress.getByName("127.0.0.1"), 9300));

        // 3. 创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog2").setTypes("article");
        // 3.1 设置查询条件(范围查询)
        // from("1", false): 开始(是否包含开始)
        // to("3", false): 结束(是否包含结束)
        searchRequestBuilder.setQuery(QueryBuilders.rangeQuery("id")
                .from("1", false).to("3", false));

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

    /** 搜索分页、排序 */
    @Test
    public void test6()throws Exception{

        // 1. 创建Settings配置信息对象
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new TransportAddress(
                InetAddress.getByName("127.0.0.1"), 9300));

        // 3. 创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog2").setTypes("article");
        // 3.1 设置查询条件(匹配查询)
        searchRequestBuilder.setQuery(QueryBuilders.matchQuery("title","服务框架"));
        // 3.2 设置分页起始数 (当前页码 - 1) * 页大小
        searchRequestBuilder.setFrom(1);
        // 3.3 设置页大小
        searchRequestBuilder.setSize(1);
        // 3.4 设置根据id排序(升序)
        searchRequestBuilder.addSort("id", SortOrder.ASC);

        // 4. 执行请求，得到搜索响应对象
        SearchResponse searchResponse = searchRequestBuilder.get();

        // 5. 获取搜索结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("总命中数：" + hits.totalHits);

        // 6. 迭代搜索结果
        for (SearchHit hit : hits) {
            System.out.println("JSON字符串：" + hit.getSourceAsString());
        }
        // 7. 释放资源
        transportClient.close();
    }

    /** 高亮显示 */
    @Test
    public void test7()throws Exception{
        // 1. 创建Settings配置信息对象
        Settings settings = Settings.builder()
                .put("cluster.name","elasticsearch").build();
        // 2. 创建ES传输客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        // 2.1 添加传输地址对象
        transportClient.addTransportAddress(new TransportAddress(
                InetAddress.getByName("127.0.0.1"), 9300));

        // 3. 创建搜索请求构建对象
        SearchRequestBuilder searchRequestBuilder = transportClient
                .prepareSearch("blog2").setTypes("article");
        // 3.1 设置查询条件
        searchRequestBuilder.setQuery(QueryBuilders.termQuery("content","开源"));

        // 3.2 创建高亮构建对象
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        // 3.2.1 设置高亮字段
        highlightBuilder.field("content");
        // 3.2.2 设置高亮格式器前缀
        highlightBuilder.preTags("<font color='red'>");
        // 3.2.3 设置高亮格式器后缀
        highlightBuilder.postTags("</font>");
        // 3.3 设置高亮对象
        searchRequestBuilder.highlighter(highlightBuilder);

        // 4. 执行请求，得到搜索响应对象
        SearchResponse searchResponse = searchRequestBuilder.get();
        // 5. 获取搜索结果
        SearchHits hits = searchResponse.getHits();
        System.out.println("总命中数：" + hits.totalHits);

        // 6. 迭代搜索结果
        for (SearchHit hit : hits) {
            // 获取高亮字段集合
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            // 获取content字段的高亮内容
            String content = highlightFields.get("content")
                    .getFragments()[0].toString();
            System.out.println(hit.getSourceAsMap().get("id") + "\t"
                    + hit.getSourceAsMap().get("title") + "\t" + content);
        }
        // 7. 释放资源
        transportClient.close();
    }
}
