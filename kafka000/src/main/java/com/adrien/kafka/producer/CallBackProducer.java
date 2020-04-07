package com.adrien.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class CallBackProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //Kafka 服务器与主机名
        properties.put("bootstrap.servers","hadoop103:9092");
        //等待所有副本应答
        properties.put("acks","1");
        //消息发送最大尝试数
        properties.put("retries",0);
        //一批消息处理大小
        properties.put("batch.size",164384);
        //请求延时
        properties.put("linger.ms",1);
        //发送缓存区的大小
        properties.put("buffer.memory",33554432);
        //key 序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0 ; i <= 20 ; i ++ ){
            kafkaProducer.send(new ProducerRecord<String, String>("adrien", "Hello World " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        System.err.println(recordMetadata.offset() + "==" + " 分区" + recordMetadata.partition());
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
