package com.shanghaiuniversity.mapperreduce.mr;

import com.shanghaiuniversity.mapperreduce.model.dim.base.BaseDimension;
import com.shanghaiuniversity.mapperreduce.model.value.BaseStatsValueWritable;
import com.shanghaiuniversity.mapperreduce.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * 自定义的配合自定义output进行具体sql输出的类
 * 
 * @author root
 *
 */
public interface IOutputCollector {

    /**
     * 具体执行统计数据插入的方法
     * 
     * @param conf
     * @param key
     * @param value
     * @param pstmt
     * @param converter
     * @throws SQLException
     * @throws IOException
     */
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException;
}
