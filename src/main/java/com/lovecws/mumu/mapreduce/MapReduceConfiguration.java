package com.lovecws.mumu.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: mapreduce
 * @date 2017-10-10 12:47
 */
public class MapReduceConfiguration {

    private static final Logger log = Logger.getLogger(MapReduceConfiguration.class);

    public static String HDFS_URI = "hdfs://192.168.11.25:9000";

    /**
     * 获取到分布式文件
     *
     * @return
     */
    public String url() {
        String hadoop_address = System.getenv("HADOOP_ADDRESS");
        if (hadoop_address != null) {
            HDFS_URI = hadoop_address;
        }
        return HDFS_URI;
    }

    /**
     * 获取到hdfs分布式文件系统
     *
     * @return
     */
    public DistributedFileSystem distributedFileSystem() {
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        try {
            distributedFileSystem.initialize(new URI(HDFS_URI), new Configuration());
        } catch (IOException | URISyntaxException e) {
            log.error(e);
        }
        return distributedFileSystem;
    }

    /**
     * 关闭资源
     *
     * @param distributedFileSystem
     */
    public void close(DistributedFileSystem distributedFileSystem) {
        if (distributedFileSystem != null) {
            try {
                distributedFileSystem.close();
            } catch (IOException e) {
                log.error(e);
            }
        }
    }

    /**
     * 查看输出结果
     *
     * @param path
     */
    public void print(String path) {
        log.info("mapreduce输出结果:...................................................");
        DistributedFileSystem distributedFileSystem = distributedFileSystem();
        try {
            FileStatus[] fileStatuses = distributedFileSystem.listStatus(new Path(path));
            for (FileStatus fs : fileStatuses) {
                log.info(fs);
                FSDataInputStream fsDataInputStream = distributedFileSystem.open(fs.getPath());
                byte[] bs = new byte[fsDataInputStream.available()];
                fsDataInputStream.read(bs);
                log.info("\n" + new String(bs) + "\n");
            }
        } catch (IOException e) {
            log.error(e);
        } finally {
            close(distributedFileSystem);
        }
    }

    public void cluster() {

    }
}
