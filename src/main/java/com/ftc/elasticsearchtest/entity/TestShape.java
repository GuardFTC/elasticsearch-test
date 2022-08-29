package com.ftc.elasticsearchtest.entity;

import cn.hutool.json.JSONObject;
import lombok.Data;

/**
 * @author: 冯铁城 [17615007230@163.com]
 * @date: 2022-08-29 10:50:31
 * @describe: 图形信息
 */
@Data
public class TestShape {

    /**
     * 名称
     */
    private String name;

    /**
     * 图形
     */
    private JSONObject location;
}
