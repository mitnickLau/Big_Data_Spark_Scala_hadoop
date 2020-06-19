/**
 * Copyright (C), 2018-2020
 * FileName: WordCountDriver
 * Author:   xjl
 * Date:     2020/6/19 15:00
 * Description: mr的运行的主类 本类中组合和一些程序的运行的所需要的信息
 * 比如是使用的是mapper 类 那个reduce类 输入数据在哪里 输出数据在什么地方
 */
package com.shanghaiuniversity.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        //通过job来封装本次的mr的相关信息
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);
        //指定运行的主类
        job.setJarByClass(WordCountDriver.class);
        //指定我这个 job 所在的 jar 包
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定的mr的输入的数据的路径和最终的输出的结果的存放的位置
        FileInputFormat.setInputPaths(job, "E:\\GItHub_project\\Big_Data\\hadoop_parent\\src\\main\\resources\\input");
        //指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(job, new Path("E:\\GItHub_project\\Big_Data\\hadoop_parent\\src\\main\\resources\\output"));
        //向 yarn 集群提交这个 job 表示的监控打印的job 的计划
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
