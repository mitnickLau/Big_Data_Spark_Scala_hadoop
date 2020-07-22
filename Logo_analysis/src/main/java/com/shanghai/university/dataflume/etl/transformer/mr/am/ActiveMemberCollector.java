package com.shanghai.university.dataflume.etl.transformer.mr.am;

import com.shanghai.university.dataflume.etl.common.GlobalConstants;
import com.shanghai.university.dataflume.etl.common.KpiType;
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


/**
 * 定义具体的active member kpi的输出类
 * 
 * @author root
 *
 */
public class ActiveMemberCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        // 第一步: 将key&value进行强制转换
        StatsUserDimension statsUser = (StatsUserDimension) key;
        IntWritable activeMembers = (IntWritable) ((MapWritableValue) value).getValue().get(new IntWritable(-1));

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getStatsCommon().getDate()));
        if (KpiType.BROWSER_ACTIVE_MEMBER.name.equals(statsUser.getStatsCommon().getKpi().getKpiName())) {
            // 表示输出结果是统计browser active member的，那么进行browser维度信息设置
            pstmt.setInt(++i, converter.getDimensionIdByValue(statsUser.getBrowser()));
        }
        pstmt.setInt(++i, activeMembers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeMembers.get());

        // 将pstmt添加到批量执行中
        pstmt.addBatch();
    }

}
