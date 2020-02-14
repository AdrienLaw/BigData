package com.adrien.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //压缩
        //compress("/Users/luohaotian/Downloads/激活码","org.apache.hadoop.io.compress.BZip2Codec");
        //compress("/Users/luohaotian/Downloads/激活码","org.apache.hadoop.io.compress.GzipCodec");
        //compress("/Users/luohaotian/Downloads/激活码","org.apache.hadoop.io.compress.DefaultCodec");
        decompress("/Users/luohaotian/Downloads/激活码.bz2");
    }

    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        //1. 获取输入流
        FileInputStream fileInputStream = new FileInputStream(new File(fileName));
        //2. 获取输出流
        //获取压缩方式扩展名
        Class<?> classCodec = Class.forName(method);
        CompressionCodec classc = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());

        FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName + classc.getDefaultExtension()));
        //有压缩方式的输出流
        CompressionOutputStream classcOutputStream = classc.createOutputStream(fileOutputStream);
        //3. 流的对拷 false 结束时不关闭流
        IOUtils.copyBytes(fileInputStream,classcOutputStream,1024 * 1024,false);
        //4. 关闭资源
        IOUtils.closeStream(classcOutputStream);
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fileInputStream);
    }

    private static void decompress(String fileName) throws IOException {
        //1. 压缩方式检查
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = codecFactory.getCodec(new Path(fileName));
        //校验是否可以压缩
        if (codec == null){
            //不能处理
            return;
        }
        //2. 获取输入流
        FileInputStream fileInputStream = new FileInputStream(new File(fileName));
        CompressionInputStream codecInputStream = codec.createInputStream(fileInputStream);
        //3. 获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File(fileName + ".decode"));
        //4. 流的对拷
        IOUtils.copyBytes(codecInputStream,fileOutputStream,1024 * 1024,false);
        //关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(codecInputStream);
        IOUtils.closeStream(fileInputStream);
    }
}
