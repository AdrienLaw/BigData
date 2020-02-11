package com.adrien.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //key 是手机号
        //value 流量信息
        //获取前三位
        String preTelNum = text.toString().substring(0, 3);
        int partition = 4;
        if ("136".equals(preTelNum)) {
            partition = 0;
        } else if ("137".equals(preTelNum)) {
            partition = 1;
        } else if ("138".equals(preTelNum)) {
            partition = 2;
        } else if ("139".equals(preTelNum)) {
            partition = 3;
        }
        return partition;
    }
}
