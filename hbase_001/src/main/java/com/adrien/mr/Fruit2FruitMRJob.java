package com.adrien.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fruit2FruitMRJob extends Configured implements Tool {


    //创建Hadoop 以及 HBase管理配置对象
    public static Configuration conf;

    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_POS = "100.168.1.182";
    private static final String ZK_POS = "Master:2181,Slave0:2181,Slave1:2181,Slave2:2181";
    private static final String ZK_PORT_VALUE = "2181";

    static{
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop103"); // 换成自己的ZK节点IP
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT_VALUE);
    }
    public static void main(String[] args) {
        Fruit2FruitMRJob fruitMRJob = new Fruit2FruitMRJob();
        try {
            int run = ToolRunner.run(conf, fruitMRJob, args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        //组装 Job
        Job job = Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(Fruit2FruitMRJob.class);
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);
        TableMapReduceUtil.initTableMapperJob(
                "fruit",//Mapper 操作表名
                scan, //扫描对象是谁
                ReadFruitMapper.class, //制定Mapper
                ImmutableBytesWritable.class, //制定Mapper的输出key
                Put.class, //制定Mapper的输出value
                job //制定job
        );
        TableMapReduceUtil.initTableReducerJob(
                "fruit_mr",
                WriteFruitMRReducer.class,
                job
        );
        job.setNumReduceTasks(1);
        boolean completion = job.waitForCompletion(true);
        return completion ? 0 : 1;

    }
}
