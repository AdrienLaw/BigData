package com.arien.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private Map<String,Integer> map = new HashMap<String,Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple input) {
        //获取传递过来的数据
        String word = input.getString(0);
        Integer num = input.getInteger(1);
        //累加单词
        if (map.containsKey(word)) {
            Integer count = map.get(word);
            map.put(word,count + num);
        } else {
            map.put(word,num);
        }
        System.err.println(Thread.currentThread().getId() + " word:" + word + " num:" + map.get(word));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
