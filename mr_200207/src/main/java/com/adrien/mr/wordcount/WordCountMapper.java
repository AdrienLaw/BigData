package com.adrien.mr.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN 输入数据的key
 * VALUEIN 输入数据的value
 * KEYOUT 输出数据的key的类型
 * VALUEOUT 输出数据的value的类型
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text textKey = new Text();
    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        System.out.println(key.toString());
        //1. 获取一行
        String line = value.toString();
        //2. 切割单词
        String[] words = line.split(" ");
        //3. 循环写出
        for (String word : words) {
            Text textKey = new Text();
            IntWritable intWritable = new IntWritable();
            textKey.set(word);
            intWritable.set(1);
            context.write(textKey,intWritable);
        }
    }
}
