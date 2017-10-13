package com.lovecws.mumu.mapreduce.mapred.temperature;

import com.lovecws.mumu.mapreduce.MapReduceConfiguration;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Date;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-12 14:57
 */
public class MaxTemperatureMapReduceTest {

    private static final Logger log = Logger.getLogger(MaxTemperatureMapReduceTest.class);

    @Test
    public void mapreduce() {
        MapReduceConfiguration mapReduceConfiguration = new MapReduceConfiguration();
        try {
            String inputPath = mapReduceConfiguration.url() + "/mapreduce/temperature/input";
            String outputPath = mapReduceConfiguration.url() + "/mapreduce/temperature/mapreduce/output" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmSS");
            MaxTemperatureMapReduce.main(new String[]{inputPath, outputPath});
            mapReduceConfiguration.print(outputPath);
        } catch (Exception e) {
            log.error(e);
        }
    }
}
