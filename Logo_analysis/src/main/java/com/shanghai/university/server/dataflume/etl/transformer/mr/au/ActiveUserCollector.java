package com.shanghai.university.server.dataflume.etl.transformer.mr.au;

import com.shanghai.university.server.dataflume.etl.common.GlobalConstants;
import com.shanghai.university.server.dataflume.etl.transformer.model.dim.StatsUserDimension;
import com.shanghai.university.server.dataflume.etl.transformer.model.dim.base.BaseDimension;
import com.shanghai.university.server.dataflume.etl.transformer.model.value.BaseStatsValueWritable;
import com.shanghai.university.server.dataflume.etl.transformer.model.value.reduce.MapWritableValue;
import com.shanghai.university.server.dataflume.etl.transformer.mr.IOutputCollector;
import com.shanghai.university.server.dataflume.etl.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class ActiveUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        // 进行强制后获取对应的值
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeUserValue = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1));

        // 进行参数设置
        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        pstmt.setInt(++i, activeUserValue.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeUserValue.get());

        // 添加到batch中
        pstmt.addBatch();
    }
}
