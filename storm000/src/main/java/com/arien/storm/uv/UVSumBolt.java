package com.arien.storm.uv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class UVSumBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private Map<String,Integer> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        // 1 获取传递过来的数据
        String ip = tuple.getString(0);
        Integer num = tuple.getInteger(1);
        // 2 累加单词
        if (map.containsKey(ip)) {
            Integer count = map.get(ip);
            map.put(ip,count+ num);
        } else {
            map.put(ip,num);
        }
        System.err.println(Thread.currentThread().getId() + "IP: " + ip + "num: " + map.get(ip));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
