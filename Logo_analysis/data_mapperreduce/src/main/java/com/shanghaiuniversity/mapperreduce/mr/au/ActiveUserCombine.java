package com.shanghaiuniversity.mapperreduce.mr.au;

import com.shanghaiuniversity.mapperreduce.model.dim.StatsUserDimension;
import com.shanghaiuniversity.mapperreduce.model.value.map.TimeOutputValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



/**
 * combine类
 * 
 * @author root
 *
 */
public class ActiveUserCombine extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, TimeOutputValue> {
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        for (TimeOutputValue tov : values) {
            context.write(key, tov);
        }
    }
}
