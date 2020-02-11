package com.adrien.mr.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //输出分行展示
        String line = key.toString();
        line = line + "\r\t";
        text.set(line);
        //防止有重复数据
        for (NullWritable value : values) {
            context.write(key,NullWritable.get());
        }
    }
}
