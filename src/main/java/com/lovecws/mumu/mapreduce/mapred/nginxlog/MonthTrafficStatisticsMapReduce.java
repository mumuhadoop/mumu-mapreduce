package com.lovecws.mumu.mapreduce.mapred.nginxlog;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 月统计
 * @date 2017-10-13 10:09
 */
public class MonthTrafficStatisticsMapReduce {

    /**
     * mapper
     */
    public static class MonthTrafficStatisticsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            Map<String, Object> nginxLogMap = NginxAccessLogParser.parseLine(value.toString());
            String remoteAddr = nginxLogMap.get("remoteAddr").toString();
            Date accessTimeDate = (Date) nginxLogMap.get("accessTime");
            String accessTime = DateFormatUtils.format(accessTimeDate, "yyyyMM");
            context.write(new Text(accessTime), new Text(remoteAddr));
        }
    }

    /**
     * reduce
     */
    public static class MonthTrafficStatisticsReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Set<String> ipSet = new HashSet<String>();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                ipSet.add(iterator.next().toString());
            }
            context.write(key, new IntWritable(ipSet.size()));
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: Month Traffic Statistics <input path> <output path>");
            System.exit(-1);
        }
        String nginxLogInput = args[0];
        String nginxLogOutput = args[1];

        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration);
            job.setJobName("MonthTrafficStatistics");

            job.setJarByClass(MonthTrafficStatisticsMapReduce.class);

            FileInputFormat.addInputPath(job, new Path(nginxLogInput));
            FileOutputFormat.setOutputPath(job, new Path(nginxLogOutput));

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapperClass(MonthTrafficStatisticsMapper.class);
            job.setReducerClass(MonthTrafficStatisticsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
