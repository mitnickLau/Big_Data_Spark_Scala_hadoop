package com.shanghaiuniversity.mapperreduce.hive;

import com.shanghaiuniversity.etl.common.DateEnum;
import com.shanghaiuniversity.etl.util.TimeUtil;
import com.shanghaiuniversity.mapperreduce.model.dim.base.DateDimension;
import com.shanghaiuniversity.mapperreduce.service.IDimensionConverter;
import com.shanghaiuniversity.mapperreduce.service.impl.DimensionConverterImpl;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;



/**
 * 操作日期dimension 相关的udf
 * 
 * @author root
 *
 */
public class DateDimensionUDF extends UDF {
    private IDimensionConverter converter = new DimensionConverterImpl();

   

    /**
     * 根据给定的日期（格式为:yyyy-MM-dd）至返回id
     * 
     * @param day
     * @return
     */
    public IntWritable evaluate(Text day) {
        DateDimension dimension = DateDimension.buildDate(TimeUtil.parseString2Long(day.toString()), DateEnum.DAY);
        try {
            int id = this.converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
