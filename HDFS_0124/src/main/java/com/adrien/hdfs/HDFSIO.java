package com.adrien.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSIO {

    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2. 创建输入流
        FileInputStream fileInputStream = new FileInputStream(new File("/Users/luohaotian/Downloads/jdk-8u231-linux-x64.tar.gz"));
        //3. 获取输出流
        FSDataOutputStream dataOutputStream = fileSystem.create(new Path("/0529/jdk-8u231-linux-x64.tar.gz"));
        //4. 流对拷
        IOUtils.copyBytes(fileInputStream,dataOutputStream,conf);
        //5. 关闭资源
        IOUtils.closeStream(dataOutputStream);
        IOUtils.closeStream(fileInputStream);
        fileSystem.close();
    }



    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1. 获取文件系统
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2. 创建输入流
        FSDataInputStream dataInputStream = fileSystem.open(new Path("/ximenqing.txt"));
        //3. 获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/luohaotian/Downloads/geektime/西门大鳏人.txt"));
        //4. 流对拷
        IOUtils.copyBytes(dataInputStream,fileOutputStream,conf);
        //5. 关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(dataInputStream);
        fileSystem.close();
    }


    //下载第一块
    @Test
    public void readFileSeek1() throws Exception{
        //1. 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2. 创建输入流
        FSDataInputStream dataInputStream = fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        //3. 获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/luohaotian/Downloads/hadoop-2.7.2.tar.gz.part1"));
        //4. 流对拷
        byte[] bytes = new byte[1024];
        for (int i = 0; i < 1024*128; i++) {
            dataInputStream.read(bytes);
            fileOutputStream.write(bytes);

        }
        //5. 关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(dataInputStream);
        fileSystem.close();
    }


    //下载第二块
    @Test
    public void readFileSeek2() throws Exception{
        //1. 获取文件系统
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
        //2. 创建输入流
        FSDataInputStream dataInputStream = fileSystem.open(new Path("/hadoop-2.7.2.tar.gz"));
        //3. 获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/luohaotian/Downloads/hadoop-2.7.2.tar.gz.part2"));
        //5. 设置读取起点
        dataInputStream.seek(1024 * 1024 *128);
        //4. 流对拷

        IOUtils.copyBytes(dataInputStream,fileOutputStream,conf);
        //5. 关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(dataInputStream);
        fileSystem.close();
    }



}
