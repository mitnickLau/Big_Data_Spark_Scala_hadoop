package com.shanghai.university.dataflume.etl.transformer.mr.nu;

import com.shanghai.university.dataflume.etl.common.GlobalConstants;
import com.shanghai.university.dataflume.etl.transformer.model.dim.StatsUserDimension;
import com.shanghai.university.dataflume.etl.transformer.model.dim.base.BaseDimension;
import com.shanghai.university.dataflume.etl.transformer.model.value.BaseStatsValueWritable;
import com.shanghai.university.dataflume.etl.transformer.model.value.reduce.MapWritableValue;
import com.shanghai.university.dataflume.etl.transformer.mr.IOutputCollector;
import com.shanghai.university.dataflume.etl.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class StatsDeviceBrowserNewInstallUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1));

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.addBatch();
    }

}
