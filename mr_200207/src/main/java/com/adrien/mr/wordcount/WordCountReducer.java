package com.adrien.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN 输入数据的key
 * VALUEIN 输入数据的value
 * KEYOUT 输出数据的key的类型
 * VALUEOUT 输出数据的value的类型
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {


    IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        //1. 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        intWritable.set(sum);

        //2. 写出
        context.write(key,intWritable);

    }
}
