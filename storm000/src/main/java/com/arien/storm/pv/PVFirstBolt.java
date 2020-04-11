package com.arien.storm.pv;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PVFirstBolt implements IRichBolt {
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    private long pv = 0;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        // 获取传递过来的数据
        String logLIne = tuple.getString(0);
        //截取 session_ID
        String session_ID = logLIne.split("\t")[1];
        if (session_ID != null) {
            pv ++;
        }
        // 提交
        collector.emit(new Values(Thread.currentThread().getId(),pv));
        System.err.println("ThreadID: " + Thread.currentThread().getId() + " PV: " + pv);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ThreadID","pv"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
