package com.shanghaiuniversity.mr.cache;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	HashMap<String, String> pdMap = new HashMap<>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		// 缓存小表
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		
		String line;
		while(StringUtils.isNotEmpty(line = reader.readLine())){
//			pid	pname
//			01	小米

			// 1 切割
			String[] fileds = line.split("\t");
			
			pdMap.put(fileds[0], fileds[1]);
		}
		
		// 2 关闭资源
		IOUtils.closeStream(reader);
	}
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
//		id	pid	amount
//		1001	01	1
		
//		pid	pname
//		01	小米
		// 1 获取一行
		String line = value.toString();
		
		// 2 切割
		String[] fileds = line.split("\t");
		
		// 3 获取pid
		String pid = fileds[1];
		
		// 4 取出pname
		String pname = pdMap.get(pid);
		
		// 5 拼接
		line = line +"\t"+ pname;
		
		
		k.set(line);
		
		// 6 写出
		context.write(k, NullWritable.get());
	}
}
