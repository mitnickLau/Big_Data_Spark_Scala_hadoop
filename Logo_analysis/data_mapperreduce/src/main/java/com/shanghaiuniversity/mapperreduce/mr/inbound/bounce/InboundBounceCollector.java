package com.shanghaiuniversity.mapperreduce.mr.inbound.bounce;

import com.shanghaiuniversity.etl.common.GlobalConstants;
import com.shanghaiuniversity.mapperreduce.model.dim.StatsInboundDimension;
import com.shanghaiuniversity.mapperreduce.model.dim.base.BaseDimension;
import com.shanghaiuniversity.mapperreduce.model.value.BaseStatsValueWritable;
import com.shanghaiuniversity.mapperreduce.model.value.reduce.InboundBounceReduceValue;
import com.shanghaiuniversity.mapperreduce.mr.IOutputCollector;
import com.shanghaiuniversity.mapperreduce.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;



public class InboundBounceCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsInboundDimension inboundDimension = (StatsInboundDimension) key;
        InboundBounceReduceValue inboundReduceValue = (InboundBounceReduceValue) value;

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(inboundDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, inboundDimension.getInbound().getId()); // 直接设置，在mapper类中已经设置
        pstmt.setInt(++i, inboundReduceValue.getBounceNumber());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, inboundReduceValue.getBounceNumber());

        pstmt.addBatch();
    }

}
