package com.arien.storm.uv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class UVFirstBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        // 1 获取传递过来的一行数据
        String line = tuple.getString(0);
        // 2 截取
        String[] split = line.split("\t");
        String ip = split[3];
        // 3 发射
        collector.emit(new Values(ip,1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));

    }
}
