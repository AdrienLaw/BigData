package com.adrien.mr.keyvaluetext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyValueTextMapper extends Mapper<Text,Text,Text, IntWritable> {
    IntWritable intWritable = new IntWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //1. 封装对象
        //2. 写出
        context.write(key,intWritable);
    }
}
