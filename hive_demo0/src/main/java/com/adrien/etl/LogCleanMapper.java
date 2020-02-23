package com.adrien.etl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogCleanMapper extends Mapper<LongWritable, Text, LongWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] splits = line.split("\t");
        if (splits.length < 32) {
            context.getCounter("Web Data ETL","Data Length is too short");
            return;
        }

        String url = splits[1];
        if (StringUtils.isBlank(url)) {
            context.getCounter("Web Data ETL","url is Blank").increment(1L);
            return;
        }

        String provinceId = splits[20];
        int provinceId_Int = Integer.MAX_VALUE;
        try {
            Integer.parseInt(provinceId);
        } catch (Exception e){
            context.getCounter("Web Data ETL","provinceId is UNKNOWN").increment(1L);
            return;
        }
        if (StringUtils.isBlank(provinceId)  || provinceId_Int == Integer.MAX_VALUE) {
            context.getCounter("Web Data ETL","provinceId is UNKNOWN").increment(1L);
            return;
        }

        context.write(key,value);
    }
}
