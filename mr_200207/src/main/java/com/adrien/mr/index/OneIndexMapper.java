package com.adrien.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OneIndexMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text textKey = new Text();
    IntWritable intWritable = new IntWritable();
    String name;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1. 获取一行
        String line = value.toString();
        //2. 切割
        String[] fields = line.split(" ");
        //3. 写出
        for (String field : fields) {
            textKey.set(field+"--"+name);
            intWritable.set(1);
            context.write(textKey,intWritable);
        }
    }
}
