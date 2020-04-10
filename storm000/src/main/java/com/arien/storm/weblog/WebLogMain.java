package com.arien.storm.weblog;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class WebLogMain {
    public static void main(String[] args) {
        // 1 创建拓扑对象
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // 2 设置Spout和bolt
        topologyBuilder.setSpout("WebLogSpout",new WebLogSpout(),1);
        topologyBuilder.setBolt("WebLogBolt",new WebLogBolt(),4).shuffleGrouping("WebLogSpout");
        // 3 配置Worker开启个数
        Config config = new Config();
        config.setNumWorkers(2);
        if (args.length > 0) {
            try {
                // 4 分布式提交
                StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 5 本地模式提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("weblogtopology",config,topologyBuilder.createTopology());
        }

    }
}
