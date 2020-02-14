package com.adrien.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text,Text,Text,Text> {

    Text textValue = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer stringBuffer = new StringBuffer();

        for (Text value : values) {
            stringBuffer.append(value.toString().replace("\t","-->")+"\t");
        }
        //写出
        textValue.set(stringBuffer.toString());
        context.write(key,textValue);
    }
}
