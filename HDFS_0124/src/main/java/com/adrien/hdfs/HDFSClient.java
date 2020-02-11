package com.adrien.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://172.16.120.149:9000");

        //FileSystem fileSystem = FileSystem.get(conf);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");

        fileSystem.mkdirs(new Path("/0529/dashen/Adrien"));

        fileSystem.close();

    }



    //文件上传
    @Test
    public void testCopyFromLocalFile(){
        //1. 获取HDFS对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");

       try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
            //2. 执行上传API
            fileSystem.copyFromLocalFile(new Path("/Users/luohaotian/Downloads/hadoop-2.7.2-src.tar.gz"),new Path("/0529/dashen/Adrien"));

            //3. 关闭资源
            fileSystem.close();
            System.out.println("======SUCCESS======");
       } catch (Exception e){
           System.out.println("=====FAIL=====");
       }
    }


    @Test
    public void testCopyToLocalFile(){
        //1. 获取HDFS对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");

        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
            //2. 执行下载API
            //fileSystem.copyToLocalFile(new Path("/panjinlian.txt"),new Path("/Users/luohaotian/Downloads"));
            /**
             * @param delSrc
             *          whether to delete the src
             * @param src
             *          path
             * @param dst
             *          path
             * @param useRawLocalFileSystem
             *          whether to use RawLocalFileSystem as local file system or not.
             *
             */
            fileSystem.copyToLocalFile(false,new Path("/panjinlian.txt"),new Path("/Users/luohaotian/Downloads/ximenqing.txt"),true);
            //3. 关闭资源
            fileSystem.close();
            System.out.println("======SUCCESS======");
        } catch (Exception e){
            System.out.println("=====FAIL=====");
        }
    }

    @Test
    public void testDelete(){
        //1. 获取HDFS对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");

        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
            //2 执行删除
            fileSystem.delete(new Path("/0529/dashen"),true);
            //3. 关闭资源
            fileSystem.close();
            System.out.println("======SUCCESS======");
        } catch (Exception e){
            System.out.println("=====FAIL=====");
        }
    }


    @Test
    public void testRename(){
        //1. 获取HDFS对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");

        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
            //2 修改文件名称
            fileSystem.rename(new Path("/panjinlian.txt"),new Path("/ximenqing.txt"));
            //3. 关闭资源
            fileSystem.close();
            System.out.println("======SUCCESS======");
        } catch (Exception e){
            System.out.println("=====FAIL=====");
        }
    }


    @Test
    public void testListFiles(){
        //1. 获取HDFS对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");

        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
            //2 修改文件名称
            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
            while (listFiles.hasNext()){
                LocatedFileStatus fileStatus = listFiles.next();
                System.out.println(fileStatus.getGroup()+"         =====分组=====");
                System.out.println(fileStatus.getPermission()+"         =====权限=====");
                BlockLocation[] blockLocations = fileStatus.getBlockLocations();
                for (BlockLocation blockLocation : blockLocations) {
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println(host+"          =====block端口号=====");
                    }
                }

                System.out.println("---------------------------");


            }
            //3. 关闭资源
            fileSystem.close();
            System.out.println("======SUCCESS======");
        } catch (Exception e){
            System.out.println("=====FAIL=====");
        }
    }

    @Test
    public void testListStatus(){
        //1. 获取HDFS对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");

        try {
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),conf,"root");
            //2 修改文件名称
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()){
                    System.out.println("f: "+fileStatus.getPath().getName());
                } else {
                    System.out.println("d: "+fileStatus.getPath().getName());

                }
            }
            //3. 关闭资源
            fileSystem.close();
            System.out.println("======SUCCESS======");
        } catch (Exception e){
            System.out.println("=====FAIL=====");
        }
    }


}

