package com.lovecws.mumu.mapreduce.mapred.nginxlog;

import com.lovecws.mumu.mapreduce.MapReduceConfiguration;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 月日志访问统计
 * @date 2017-10-13 10:25
 */
public class MonthTrafficStatisticsMapReduceTest {
    private static final Logger log = Logger.getLogger(MonthTrafficStatisticsMapReduceTest.class);

    @Test
    public void mapreduce() {
        MapReduceConfiguration mapReduceConfiguration = new MapReduceConfiguration();
        long start = System.currentTimeMillis();
        log.info("开始计算nginx月访问日志IP统计量");
        try {
            String inputPath = mapReduceConfiguration.url() + "/mapreduce/nginxlog/access/input";
            String outputPath = mapReduceConfiguration.url() + "/mapreduce/nginxlog/access/output/month" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
            MonthTrafficStatisticsMapReduce.main(new String[]{inputPath, outputPath});
            mapReduceConfiguration.print(outputPath);
        } catch (Exception e) {
            log.error(e);
        }
        long end = System.currentTimeMillis();
        log.info("运行mapreduce程序花费时间:" + (end - start) / 1000 + "s");
    }
}
