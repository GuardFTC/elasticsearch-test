package com.ftc.elasticsearchtest.entity;

import lombok.Data;

import java.util.List;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-22 15:54:07
 * @describe: 查询测试使用Student实体类
 */
@Data
public class SearchStudent {

    /**
     * 名称
     */
    private String name;

    /**
     * 年龄
     */
    private Integer age;

    /**
     * 成绩
     */
    private Double grade;

    /**
     * 描述
     */
    private String des;

    /**
     * 标签
     */
    private List<String> tags;
}
