package com.shanghai.university.dataflume.etl.transformer.mr.au;

import com.shanghai.university.dataflume.etl.transformer.model.dim.StatsUserDimension;
import com.shanghai.university.dataflume.etl.transformer.model.value.map.TimeOutputValue;
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
