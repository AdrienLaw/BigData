package com.adrien.mr2;

import com.adrien.mr.Fruit2FruitMRJob;
import com.adrien.mr.WriteFruitMRReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HDFS2HBaseDriver extends Configured implements Tool {

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
        conf.set("hbase.zookeeper.quorum", "hadoop102"); // 换成自己的ZK节点IP
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT_VALUE);
    }

    public static void main(String[] args) {
        HDFS2HBaseDriver hdfs2HBaseDriver = new HDFS2HBaseDriver();
        try {
            int run = ToolRunner.run(conf, hdfs2HBaseDriver, args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //得到Configuration
        Configuration conf = this.getConf();

        //创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(HDFS2HBaseDriver.class);
//    	Path inPath = new Path("/input/fruit.txt");
        Path inPath = new Path("hdfs://hadoop102:9000/input_fruit/fruit.tsv");
        FileInputFormat.addInputPath(job, inPath);

        //设置Mapper
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_from_hdfs", WriteFruitMRFromTxtReducer.class, job);

        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw new IOException("Job running with error");
        }

        return isSuccess ? 0 : 1;
    }

}
