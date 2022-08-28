package com.ftc.elasticsearchtest.entity;

import lombok.Data;

import java.util.Date;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-28 18:05:27
 * @describe: 网站请求数据
 */
@Data
public class WebsiteRequest {

    /**
     * 延迟 单位ms
     */
    private long latency;

    /**
     * 所在地区
     */
    private String zone;

    /**
     * 时间戳
     */
    private Date timestamp;
}
