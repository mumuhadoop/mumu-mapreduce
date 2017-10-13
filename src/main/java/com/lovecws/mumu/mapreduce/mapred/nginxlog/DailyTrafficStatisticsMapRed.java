package com.lovecws.mumu.mapreduce.mapred.nginxlog;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 通过nginx访问日志统计每日的ip访问量
 * @date 2017-10-12 11:32
 */
public class DailyTrafficStatisticsMapRed extends Configured implements Tool {

    /**
     * mapper 将每一行的 年月日时间作为key,ip地址为value
     */
    public static class DailyTrafficStatisticsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(final LongWritable key, final Text lineValue, final OutputCollector<Text, Text> outputCollector, final Reporter reporter) throws IOException {
            Map<String, Object> nginxLogMap = NginxAccessLogParser.parseLine(lineValue.toString());
            String remoteAddr = nginxLogMap.get("remoteAddr").toString();
            String accessTime = DateFormatUtils.format((Date) nginxLogMap.get("accessTime"), "yyyyMMdd");
            outputCollector.collect(new Text(accessTime), new Text(remoteAddr));
        }
    }

    /**
     * 统计相同年月日下的 ip数量(ip去重)
     */
    public static class DailyTrafficStatisticsReduce extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(final Text key, final Iterator<Text> iterator, final OutputCollector<Text, IntWritable> outputCollector, final Reporter reporter) throws IOException {
            Set<String> ipSet = new HashSet<String>();
            while (iterator.hasNext()) {
                Text remoteAddr = iterator.next();
                ipSet.add(remoteAddr.toString());
            }
            outputCollector.collect(key, new IntWritable(ipSet.size()));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Daily Traffic Statistics <input path> <output path>");
            System.exit(-1);
        }

        String temperatureInput = args[0];
        String temperatureOutput = args[1];

        JobConf jobConf = new JobConf(getConf(), getClass());
        jobConf.setJobName("Daily Traffic");
        jobConf.setProfileEnabled(true);

        FileInputFormat.addInputPath(jobConf, new Path(temperatureInput));
        FileOutputFormat.setOutputPath(jobConf, new Path(temperatureOutput));

        jobConf.setInputFormat(TextInputFormat.class);//合并小文件
        jobConf.setOutputFormat(TextOutputFormat.class);

        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(DailyTrafficStatisticsMapper.class);
        jobConf.setReducerClass(DailyTrafficStatisticsReduce.class);

        JobClient.runJob(jobConf);
        return 0;
    }
}
