package com.adrien.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text,FlowBean,Text> {
    FlowBean flowBean = new FlowBean();
    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] fields = line.split("\t");
        //封装对象
        String phoneNum = fields[0];
        long upFlow = Long.valueOf(fields[1]);
        long downFlow = Long.valueOf(fields[2]);
        long sumFlow = Long.valueOf(fields[3]);
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setSumFlow(sumFlow);
        text.set(phoneNum);
        context.write(flowBean,text);
    }
}
