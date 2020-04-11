package com.arien.storm.uv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class UVMain {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("UVSpout",new UVSpout(),1);
        builder.setBolt("UVFirstBolt",new UVFirstBolt(),4).shuffleGrouping("UVSpout");
        builder.setBolt("",new UVSumBolt(),1).shuffleGrouping("UVFirstBolt");
        Config config = new Config();
        config.setNumWorkers(2);
        if (args.length> 0) {
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("UVTopology",config,builder.createTopology());
        }
    }
}
