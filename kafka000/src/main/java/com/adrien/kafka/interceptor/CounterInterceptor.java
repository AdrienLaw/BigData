package com.adrien.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String,String> {

    private int errorCounter = 0;
    private int successCounter = 0;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    // 统计成功和失败的次数
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            successCounter ++ ;
        } else {
            errorCounter ++ ;
        }
    }

    @Override
    public void close() {
        System.out.println("Successful :" + successCounter);
        System.out.println("error :" + errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
