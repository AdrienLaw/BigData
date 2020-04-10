package com.arien.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountMain {
    public static void main(String[] args) {
        //1. 准备一个 TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("WordCountSpout",new WordCountSpout(),1);
        builder.setBolt("WordCountSplitBolt",new WordCountSplitBolt(),2).shuffleGrouping("WordCountSpout");
        builder.setBolt("WordCountBolt",new WordCountBolt(),4).fieldsGrouping("WordCountSplitBolt",new Fields("word"));
        // 2、创建一个configuration，用来指定当前topology 需要的worker的数量
        Config config = new Config();
        config.setNumWorkers(2);
        // 3、提交任务 -----两种模式 本地模式和集群模式
        if (args.length > 0) {
            // 4 分布式提交
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            // 5 本地模式提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wordcounttopology",config,builder.createTopology());
        }

    }
}
