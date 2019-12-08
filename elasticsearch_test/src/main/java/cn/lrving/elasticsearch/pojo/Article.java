package cn.lrving.elasticsearch.pojo;

import lombok.Data;

@Data
public class Article {
    private long id;
    private String title;
    private String content;
}
