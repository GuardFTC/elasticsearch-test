package com.ftc.elasticsearchtest.entity;

import lombok.Data;

import java.util.Date;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-24 19:42:52
 * @describe: 汽车订单
 */
@Data
public class CarOrder {

    /**
     * 订单汽车售价
     */
    private double price;

    /**
     * 订单汽车颜色
     */
    private String color;

    /**
     * 订单汽车厂商
     */
    private String make;

    /**
     * 订单售卖时间
     */
    private Date soldDate;
}
