package com.adrien.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/luohaotian/Downloads/Jennifer/HelloApp/input/combiner","/Users/luohaotian/Downloads/Jennifer/HelloApp/output/combiner"};

        Configuration conf = new Configuration();
        //1. 获取Job对象
        Job job = Job.getInstance(conf);
        //2. 设置java存储位置
        job.setJarByClass(WordCountDriver.class);
        //3. 关联Map和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //4. 设置Mapper阶段数据 key value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //job.setCombinerClass(WordcountCombiner.class);
        job.setCombinerClass(WordCountReducer.class);
        /**
         * 6. 设置输入路径和输出路径
         *
         * 输入输出参数写在 programs arguments
         */

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //7. 提交Job
        boolean completion = job.waitForCompletion(true);
        System.exit(completion ? 0 : 1);

    }
}
