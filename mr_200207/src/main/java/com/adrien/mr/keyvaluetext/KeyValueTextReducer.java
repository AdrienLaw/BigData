package com.adrien.mr.keyvaluetext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeyValueTextReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable intWritable = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
