package com.shanghaiuniversity.mapperreduce.mr.am;

import com.shanghaiuniversity.etl.common.KpiType;
import com.shanghaiuniversity.mapperreduce.model.dim.StatsUserDimension;
import com.shanghaiuniversity.mapperreduce.model.value.map.TimeOutputValue;
import com.shanghaiuniversity.mapperreduce.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * mapreduce程序中计算active member的reduce类<br/>
 * 其实就是计算每一组中去重memberid的一个个数
 * 
 * @author root
 *
 */
public class ActiveMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    private Set<String> unique = new HashSet<String>();
    private MapWritableValue outputValue = new MapWritableValue();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            // 将memberid添加到set集合中去，方便进行统计memberid的去重个数
            for (TimeOutputValue value : values) {
                this.unique.add(value.getId());
            }

            // 设置kpi
            this.outputValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));
            // 设置value
            this.map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
            this.outputValue.setValue(this.map);

            // 进行输出
            context.write(key, this.outputValue);
        } finally {
            // 清空操作
            this.unique.clear();
        }

    }
}
