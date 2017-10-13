package com.lovecws.mumu.mapreduce.mapred.nginxlog;

import com.lovecws.mumu.mapreduce.MapReduceConfiguration;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: nginx日志访问统计测试
 * @date 2017-10-12 13:10
 */
public class DailyTrafficStatisticsMapRedTest {
    private static final Logger log = Logger.getLogger(DailyTrafficStatisticsMapRedTest.class);

    @Test
    public void mapreduce() {
        MapReduceConfiguration mapReduceConfiguration = new MapReduceConfiguration();
        long start = System.currentTimeMillis();
        log.info("开始计算nginx访问日志IP统计量");
        try {
            String inputPath = mapReduceConfiguration.url() + "/mapreduce/nginxlog/access/input";
            String outputPath = mapReduceConfiguration.url() + "/mapreduce/nginxlog/access/output/daily" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
            ToolRunner.run(new DailyTrafficStatisticsMapRed(), new String[]{inputPath, outputPath});
            mapReduceConfiguration.print(outputPath);
        } catch (Exception e) {
            log.error(e);
        }
        long end = System.currentTimeMillis();
        log.info("运行mapreduce程序花费时间:" + (end - start) / 1000 + "s");
    }
}
