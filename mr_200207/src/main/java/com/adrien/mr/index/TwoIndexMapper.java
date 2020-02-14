package com.adrien.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoIndexMapper extends Mapper<LongWritable, Text,Text,Text> {

    Text textKey = new Text();
    Text textValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1。 获取一行
        String line = value.toString();
        String[] fields = line.split("--");
        //封装
        textKey.set(fields[0]);
        textValue.set(fields[1]);

        context.write(textKey,textValue);
    }
}
