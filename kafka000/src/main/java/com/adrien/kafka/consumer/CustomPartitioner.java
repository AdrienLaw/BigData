package com.adrien.kafka.consumer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomPartitioner implements Partitioner {


    //定制分区
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //控制分区
        return 1;
    }

    @Override
    public void close() {

    }

    //初始化
    @Override
    public void configure(Map<String, ?> map) {

    }
}
