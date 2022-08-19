package com.ftc.elasticsearchtest.config;

import lombok.Data;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-18 11:33:32
 * @describe: ElasticSearch数据源配置属性
 */
@Data
public class ElasticSearchProperties {

    /**
     * 主机
     */
    private String host;

    /**
     * 端口
     */
    private Integer port;

    /**
     * 用户名
     */
    private String username;

    /**
     * 密码
     */
    private String password;

    /**
     * 链接超时时间
     */
    private Integer connectTimeout;

    /**
     * socket链接超时时间
     */
    private Integer socketTimeout;

    /**
     * 请求链接超时时间
     */
    private Integer connectionRequestTimeout;
}
