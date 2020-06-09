package com.shanghaiuniversity.producer;

import com.shanghaiuniv.ct.common.bean.Producer;
import com.shanghaiuniversity.producer.bean.LocalFileProducer;
import com.shanghaiuniversity.producer.io.LocalFileDataIn;
import com.shanghaiuniversity.producer.io.LocalFileDataOut;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        //构建生产者对象
        Producer producer = new LocalFileProducer();

        producer.setIn(new LocalFileDataIn("E:\\GItHub_project\\Big_Data\\Telecom_customer_service\\ct_producer\\src\\main\\resources\\contact.log"));
        producer.setOut(new LocalFileDataOut("E:\\GItHub_project\\Big_Data\\Telecom_customer_service\\ct_producer\\src\\main\\resources\\call.log"));
        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
