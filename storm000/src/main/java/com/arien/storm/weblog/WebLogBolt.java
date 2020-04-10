package com.arien.storm.weblog;

import com.sun.org.apache.xml.internal.resolver.readers.ExtendedXMLCatalogReader;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.util.Map;

public class WebLogBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;
    private int num = 0 ;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    }

    @Override
    public void execute(Tuple tuple) {

            //// 1 获取传递过来的数据
            //valueString = tuple.getStringByField("log");
            String line = tuple.getString(0);
            String[] splits = line.split("\t");
            String session_ID = splits[1];
            // 2 统计发射行数

            num ++;
            System.err.println(Thread.currentThread().getId() + "lines: " + num + "  session_id: " + session_ID + "lineNum" + line);



    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
