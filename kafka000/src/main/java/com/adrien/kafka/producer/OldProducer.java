package com.adrien.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class OldProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list","hadoop102:9092");
        properties.put("request.required.acks","1");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        Producer<Integer,String> producer = new Producer<Integer,String>(new ProducerConfig(properties));
        KeyedMessage<Integer, String> message = new KeyedMessage<>("adrien", "hello world");
        producer.send(message);
    }
}
