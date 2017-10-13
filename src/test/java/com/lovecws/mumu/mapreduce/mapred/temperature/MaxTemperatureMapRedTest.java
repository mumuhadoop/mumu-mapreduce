package com.lovecws.mumu.mapreduce.mapred.temperature;

import com.lovecws.mumu.mapreduce.MapReduceConfiguration;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 测试
 * @date 2017-10-11 8:48
 */
public class MaxTemperatureMapRedTest {

    private static final Logger log = Logger.getLogger(MaxTemperatureMapRedTest.class);

    @Test
    public void mapreduce() {
        MapReduceConfiguration mapReduceConfiguration = new MapReduceConfiguration();
        DistributedFileSystem distributedFileSystem = mapReduceConfiguration.distributedFileSystem();
        try {
            String inputPath = mapReduceConfiguration.url() + "/mapreduce/temperature/input";
            Path inputpath = new Path(inputPath);
            //文件不存在 则将输入文件导入到hdfs中
            if (!distributedFileSystem.exists(inputpath)) {
                distributedFileSystem.mkdirs(inputpath);
                distributedFileSystem.copyFromLocalFile(true, new Path(this.getClass().getClassLoader().getResource("mapred/temperature/input").getPath()), new Path(inputPath + "/input"));
            }
            String outputPath = mapReduceConfiguration.url() + "/mapreduce/temperature/mapred/output" + DateFormatUtils.format(new Date(), "yyyyMMddHHmmSS");

            ToolRunner.run(new MaxTemperatureMapRed(), new String[]{inputPath, outputPath});
            mapReduceConfiguration.print(outputPath);
        } catch (Exception e) {
            log.error(e);
            e.printStackTrace();
        } finally {
            mapReduceConfiguration.close(distributedFileSystem);
        }
    }

    @Test
    public void mapper() {
        MaxTemperatureMapRed.MaxTemperatureMapper mapper = new MaxTemperatureMapRed.MaxTemperatureMapper();
        Text value = new Text("123456798676231190101234567986762311901012345679867623119010123456798676231190101234561+00121534567890356");
        OutputCollector outputCollector = Mockito.mock(OutputCollector.class);
        Reporter reporter = Mockito.mock(Reporter.class);
        try {
            mapper.map(null, value, new OutputCollector<Text, IntWritable>() {
                @Override
                public void collect(final Text text, final IntWritable intWritable) throws IOException {
                    log.info(text.toString() + "  " + intWritable.get());
                }
            }, null);
            log.info(outputCollector);
            Mockito.verify(outputCollector);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void reduce() {
        MaxTemperatureMapRed.MaxTemperatureReduce maxTemperatureReduce = new MaxTemperatureMapRed.MaxTemperatureReduce();
        try {
            List<IntWritable> list = new ArrayList<IntWritable>();
            list.add(new IntWritable(12));
            list.add(new IntWritable(31));
            list.add(new IntWritable(45));
            list.add(new IntWritable(23));
            list.add(new IntWritable(21));
            maxTemperatureReduce.reduce(new Text("1901"), list.iterator(), new OutputCollector<Text, IntWritable>() {
                @Override
                public void collect(final Text text, final IntWritable intWritable) throws IOException {
                    log.info(text.toString() + "  " + intWritable.get());
                }
            }, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
