package com.arien.storm.weblog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * 生成数据
 */
public class GenerateData {
    public static void main(String[] args) {
        FileOutputStream fileOutputStream;
        File logFile = new File("/Users/luohaotian/Downloads/Jennifer/BigData/website.log");
        Random random = new Random();
        // 1.网站名称
        String[] host = {"www.wangyiyun.com"};
        // 2.会话
        String[] sessionId = {"ABYH6Y4V4SCVXTG6DPB4VH9U123",
        "XXYH6YCGFJYERTT834R52FDXV9U34",
        "BBYH61456FGHHJ7JL89RG5VV9UYU7",
        "CYYH6Y2345GHI899OFG4V9U567",
        "VVVYH6Y4V4SFXZ56JIPDPB4V678"};
        //3.访问网站时间
        String[] time = {
                "2017-08-07 08:40:50",
                "2017-08-07 08:40:51",
                "2017-08-07 08:40:52",
                "2017-08-07 08:40:53",
                "2017-08-07 09:40:49",
                "2017-08-07 10:40:49",
                "2017-08-07 11:40:49",
                "2017-08-07 12:40:49"
        };
        // 4.拼接网站访问日志
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0 ; i < 40 ; i ++) {
            stringBuffer.append(host[0] + "\t" + sessionId[random.nextInt(5)] + "\t" +
                    time[random.nextInt(8)] + "\n");
        }
        // 5.判断log日志是否存在，不存在要创建
        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                System.out.println("Create logFile fail !");
            }
        }
        byte[] bytes = (stringBuffer.toString()).getBytes();
        // 6 将拼接的日志信息写到日志文件中
        try {
            fileOutputStream = new FileOutputStream(logFile);
            fileOutputStream.write(bytes);
            fileOutputStream.close();
            System.out.println("generate data over");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
