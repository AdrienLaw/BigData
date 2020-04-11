package com.arien.storm.pv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class PVMain {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("PVSpout",new PVSpout(),1);
        builder.setBolt("PVFirstBolt",new PVFirstBolt(),4).shuffleGrouping("PVSpout");
        builder.setBolt("PVSumBolt",new PVSumBolt(),1).shuffleGrouping("PVFirstBolt");
        Config config = new Config();
        config.setNumWorkers(2);
        if (args.length > 0) {
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
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("PVTopology",config,builder.createTopology());
        }
    }
}
