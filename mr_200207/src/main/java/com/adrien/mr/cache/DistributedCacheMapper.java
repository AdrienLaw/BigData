package com.adrien.mr.cache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class DistributedCacheMapper extends Mapper<LongWritable,Text, Text, NullWritable> {

    HashMap<String, String> pdMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        //缓存小表
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line;

        /**
         * pid	pname
         * 01	小米
         */
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //1. 切割
            String[] fields = line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }
        //2. 关闭资源
        IOUtils.closeStream(reader);
    }


    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * id	pid	amount
         * 1001	01	1
         *
         * pid	pname
         * 01	小米
         */
        //1. 获取一行
        String line = value.toString();
        //2. 切割
        String[] fields = line.split("\t");
        //3. 获取pid
        String pid = fields[1];
        //4. 获取pname
        String pname = pdMap.get(pid);
        //5. 拼接
        line = line + "\t" + pname;
        text.set(line);
        context.write(text,NullWritable.get());
    }
}
