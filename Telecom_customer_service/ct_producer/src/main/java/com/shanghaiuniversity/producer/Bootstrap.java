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

        if (args.length < 2) {
            System.out.println("系统参数不正确，请按照指定格式传递：java -jar Produce.jar path1 path2 ");
            System.exit(1);
        }

        //构建生产者对象
        Producer producer = new LocalFileProducer();

//        producer.setIn(new LocalFileDataIn("E:\\GItHub_project\\Big_Data\\Telecom_customer_service\\ct_producer\\src\\main\\resources\\contact.log"));
//        producer.setOut(new LocalFileDataOut("E:\\GItHub_project\\Big_Data\\Telecom_customer_service\\ct_producer\\src\\main\\resources\\call.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));

        //生产数据
        producer.produce();
        //关闭生产者对象
        producer.close();
    }
}
