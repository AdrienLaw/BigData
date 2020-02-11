package com.adrien.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream outputStreamAdrien;
    FSDataOutputStream outputStreamOthers;
    //核心业务逻辑
    public FRecordWriter(TaskAttemptContext job) {
        try {
            //1. 获取文件系统
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
            //2. 创建输出到adrien.log的输出流
            outputStreamAdrien = fileSystem.create(new Path("/Users/luohaotian/Downloads/Jennifer/HelloApp/output/outputFormat/adrien.log"));
            //3. 创建输出到others.log的输出流
            outputStreamOthers = fileSystem.create(new Path("/Users/luohaotian/Downloads/Jennifer/HelloApp/output/outputFormat/others.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //写
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //1. 判断key中有Adrien 写出到adrien.log;反之写到others.log
        if (key.toString().contains("adrien")) {
            outputStreamAdrien.write(key.toString().getBytes());
        } else {
            outputStreamOthers.write(key.toString().getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(outputStreamAdrien);
        IOUtils.closeStream(outputStreamOthers);
    }
}
