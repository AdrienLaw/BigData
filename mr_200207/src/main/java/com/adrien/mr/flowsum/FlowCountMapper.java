package com.adrien.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text,FlowBean> {

    Text text = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 获取一行
        String line = value.toString();
        //2. 切割 "\t"
        String[] fields = line.split("\t");
        //3. 封装对象
        text.set(fields[1]); //手机号
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFLow = Long.parseLong(fields[fields.length - 2]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFLow);
        //4. 写出
        context.write(text,flowBean);
    }
}
