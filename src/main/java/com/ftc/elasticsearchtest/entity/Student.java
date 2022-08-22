package com.ftc.elasticsearchtest.entity;

import lombok.Data;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-22 11:16:43
 * @describe: 学生类
 */
@Data
public class Student {

    /**
     * 主键ID
     */
    private Integer id;

    /**
     * 名称
     */
    private String name;

    /**
     * 年龄
     */
    private Integer age;
}
