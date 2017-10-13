package com.lovecws.mumu.mapreduce.mapred.temperature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 计算ncdc从1900 到2017年 每年的最高温度(使用mapred老版mapreduce)
 * @date 2017-10-12 10:12
 */
public class MaxNcdcTemperatureMapRed extends Configured implements Tool {

    public static class MaxNcdcTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(final LongWritable key, final Text value, final OutputCollector<Text, IntWritable> outputCollector, final Reporter reporter) throws IOException {
            String line = value.toString();
            String year = line.substring(15, 19);
            int temperature = Integer.parseInt(line.substring(87, 92));
            outputCollector.collect(new Text(year), new IntWritable(temperature));
        }
    }

    public static class MaxNcdcTemperatureReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(final Text key, final Iterator<IntWritable> values, final OutputCollector<Text, IntWritable> outputCollector, final Reporter reporter) throws IOException {
            int max_value = Integer.MIN_VALUE;
            while (values.hasNext()) {
                max_value = Math.max(max_value, values.next().get());
            }
            outputCollector.collect(key, new IntWritable(max_value));
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        String temperatureInput = args[0];
        String temperatureOutput = args[1];

        JobConf jobConf = new JobConf(getConf(), getClass());
        jobConf.setJobName("max temperature");
        jobConf.setProfileEnabled(true);

        FileInputFormat.addInputPath(jobConf, new Path(temperatureInput));
        FileOutputFormat.setOutputPath(jobConf, new Path(temperatureOutput));

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        jobConf.setMapperClass(MaxNcdcTemperatureMapper.class);
        jobConf.setCombinerClass(MaxNcdcTemperatureReduce.class);
        jobConf.setReducerClass(MaxNcdcTemperatureReduce.class);

        JobClient.runJob(jobConf);
        return 0;
    }
}
