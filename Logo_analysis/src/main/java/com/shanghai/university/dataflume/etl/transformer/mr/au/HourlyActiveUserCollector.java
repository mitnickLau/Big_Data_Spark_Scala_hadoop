package com.shanghai.university.dataflume.etl.transformer.mr.au;

import com.shanghai.university.dataflume.etl.common.GlobalConstants;
import com.shanghai.university.dataflume.etl.transformer.model.dim.StatsUserDimension;
import com.shanghai.university.dataflume.etl.transformer.model.dim.base.BaseDimension;
import com.shanghai.university.dataflume.etl.transformer.model.value.BaseStatsValueWritable;
import com.shanghai.university.dataflume.etl.transformer.model.value.reduce.MapWritableValue;
import com.shanghai.university.dataflume.etl.transformer.mr.IOutputCollector;
import com.shanghai.university.dataflume.etl.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class HourlyActiveUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUser = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        MapWritable map = mapWritableValue.getValue();

        // hourly_active_user
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getKpi())); // 根据kpi

        // 设置每个小时的情况
        for (i++; i < 28; i++) {
            int v = ((IntWritable) map.get(new IntWritable(i - 4))).get();
            pstmt.setInt(i, v);
            pstmt.setInt(i + 25, v);
        }

        pstmt.setString(i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.addBatch();
    }

}
