package com.adrien.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text,TableBean> {
    String name;
    TableBean tableBean = new TableBean();
    Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * id	pid	amount
         * 1001	01	1
         *
         * pid	pname
         * 01	小米
         */
        //1. 获取一行
        String line = value.toString();
        if (name.startsWith("order")) {
            String[] fields = line.split("\t");
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");

            text.set(fields[1]);
        } else {
            String[] fields = line.split("\t");
            tableBean.setPid(fields[0]);
            tableBean.setAmount(0);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("pd");
            tableBean.setId("");


            text.set(fields[0]);
        }
        //写出
        context.write(text,tableBean);
    }
}
