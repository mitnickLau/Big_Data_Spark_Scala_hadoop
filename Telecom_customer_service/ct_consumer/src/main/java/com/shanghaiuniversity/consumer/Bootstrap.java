/**
 * Copyright (C), 2018-2020
 * FileName: Bootstrap
 * Author:   xjl
 * Date:     2020/6/9 9:12
 * Description: 消费类
 */
package com.shanghaiuniversity.consumer;

import com.shanghaiuniv.ct.common.bean.Consumer;
import com.shanghaiuniversity.consumer.bean.CalllogConsumer;

import java.io.IOException;

/**
 * 启动消费者
 * 使用kafka中的消费者来获取flume来采集数据
 * 将数据存储在Hbase中
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        //创建消费者
        Consumer consumer = new CalllogConsumer();
        //消费数据
        consumer.consume();
        //关闭资源
        consumer.close();
    }
}
