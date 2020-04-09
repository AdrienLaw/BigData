package com.adrien.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        //定义输出的 Topic
        String from = "adrien";
        //定义输入的 Topic
        String into = "second";
        //设置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        StreamsConfig streamsConfig = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE",from)
                .addProcessor("PROCESS",new ProcessorSupplier<byte[],byte[]>(){

                    @Override
                    public Processor<byte[], byte[]> get() {
                        return new LogProcessor();
                    }
                },"SOURCE")
        .addSink("SINK", into, "PROCESS");
        //创建 Kafka Stream
        KafkaStreams streams = new KafkaStreams(builder, streamsConfig);
        streams.start();
    }
}
