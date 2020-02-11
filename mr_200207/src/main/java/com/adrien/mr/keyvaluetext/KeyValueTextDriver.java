package com.adrien.mr.keyvaluetext;

import com.adrien.mr.kv.KVTextMapper;
import com.adrien.mr.kv.KVTextReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KeyValueTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/keyValue","/Users/luohaotian/Downloads/Jennifer/HelloApp/output/keyValue"};
        //1. 获取Job
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR,"-");
        Job job = Job.getInstance(conf);
        //2. 设置jar路径
        job.setJarByClass(KeyValueTextDriver.class);
        //3. 管理mapper和reducer
        job.setMapperClass(KeyValueTextMapper.class);
        job.setReducerClass(KeyValueTextReducer.class);
        //4. 设置Mapper阶段数据 key value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //6. 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7. 提交Job
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);
    }
}
