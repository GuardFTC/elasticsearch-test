package com.ftc.elasticsearchtest.entity;

import lombok.Data;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-29 09:36:45
 * @describe: 省份信息
 */
@Data
public class Omission {

    /**
     * 省份名称
     */
    private String name;

    /**
     * 经纬度坐标 精度在前 维度在后
     */
    private double[] location;
}
