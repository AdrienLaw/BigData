package com.adrien.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 获取一行
        String line = value.toString();
        //2. 解析数据
        boolean result = parseLog(line,context);
        if (!result) {
            return;
        }
        //3. 解析通过 完成输出
        context.write(value,NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        //判断长度是否大于11
        String[] fields = line.split(" ");
        if (fields.length > 11) {
            context.getCounter("map","true").increment(1);
            return true;
        } else {
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
