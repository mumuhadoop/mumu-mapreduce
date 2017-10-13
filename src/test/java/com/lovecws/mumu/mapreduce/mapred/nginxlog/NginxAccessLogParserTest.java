package com.lovecws.mumu.mapreduce.mapred.nginxlog;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 测试nginx访问日志解析
 * @date 2017-10-12 12:09
 */
public class NginxAccessLogParserTest {

    private static final Logger log = Logger.getLogger(NginxAccessLogParserTest.class);

    @Test
    public void parseLine() {
        String nginxLogLine = "117.23.1.62 - - [11/Oct/2017:03:36:38 +0800] \"GET /group1/M00/00/06/eE0NXVk_sYKASp1LAAFP9ZsnLJk067.jpg HTTP/1.1\" 200 86005 \"-\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko\"";
        nginxLogLine = "103.47.136.16 - - [11/Oct/2017:03:54:19 +0800] \"GET /img/team_siwei.png HTTP/1.1\" 200 1983 \"https://www.chuasi.com/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko\"";
        //nginxLogLine = "106.38.241.143 - - [11/Oct/2017:04:30:36 +0800] \"GET /plus/list.php?channelid=-8&tid=45&infotype=0&nativeplace=2000&touzipz=500&type_trust=0&investmoney=0&investcycle=1000&annualreturn=0 HTTP/1.1\" 404 841 \"-\" \"Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)\"";
        Map<String, Object> stringStringMap = NginxAccessLogParser.parseLine(nginxLogLine);
        log.info(stringStringMap);
    }
}
