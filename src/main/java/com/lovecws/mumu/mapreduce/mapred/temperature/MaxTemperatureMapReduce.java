package com.lovecws.mumu.mapreduce.mapred.temperature;

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
import java.util.Iterator;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 计算历年来最高温度
 * @date 2017-10-12 14:41
 */
public class MaxTemperatureMapReduce {

    /**
     * mapper
     */
    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String year = line.substring(15, 19);
            int temperature = Integer.parseInt(line.substring(87, 92));
            context.write(new Text(year), new IntWritable(temperature));
        }
    }

    /**
     * reduce
     */
    public static class MaxTemperatureReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
            int max_value = Integer.MIN_VALUE;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                max_value = Math.max(max_value, iterator.next().get());
            }
            context.write(key, new IntWritable(max_value));
        }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(-1);
        }

        String temperatureInput = args[0];
        String temperatureOutput = args[1];
        try {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);
            job.setJarByClass(MaxTemperatureMapReduce.class);
            job.setJobName("MaxTemperature");
            FileInputFormat.addInputPath(job, new Path(temperatureInput));
            FileOutputFormat.setOutputPath(job, new Path(temperatureOutput));

            job.setMapperClass(MaxTemperatureMapper.class);
            job.setReducerClass(MaxTemperatureReduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
