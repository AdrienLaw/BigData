package com.arien.storm.weblog;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class WebLogSpout implements IRichSpout {

    private BufferedReader bufferedReader ;
    private SpoutOutputCollector collector ;
    private String str ;



    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/luohaotian/Downloads/Jennifer/BigData/website.log"),"UTF-8"));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        try {
            while ((str = bufferedReader.readLine()) != null) {
                // 发射出去
                collector.emit(new Values(str));
                Thread.sleep(500);
            }
        } catch (Exception e) {

        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 声明输出字段类型
        declarer.declare(new Fields("log"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
