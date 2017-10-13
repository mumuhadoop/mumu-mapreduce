package com.lovecws.mumu.mapreduce;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 配置文件测试
 * @date 2017-10-10 12:48
 */
public class MapReduceConfigurationTest {

    private static final Logger log = Logger.getLogger(MapReduceConfigurationTest.class);
    private MapReduceConfiguration mapReduceConfiguration = new MapReduceConfiguration();

    @Test
    public void url() {
        String url = mapReduceConfiguration.url();
        log.info(url);
    }

    @Test
    public void distributedFileSystem() {
        DistributedFileSystem distributedFileSystem = mapReduceConfiguration.distributedFileSystem();
        try {
            FileStatus[] fileStatuses = distributedFileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                log.info(fileStatus);
            }
        } catch (IOException e) {
            log.error(e);
        } finally {
            mapReduceConfiguration.close(distributedFileSystem);
        }
    }
}
