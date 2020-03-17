package com.adrien.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());
        //遍历 rowKey 下面的所有单元格
        for (Cell cell : value.rawCells()) {
            //如果当前访问的是 info 列族，进行下一步
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                //提取name 列
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    //如果数据需要q清洗和转换则需要去除具体数据，然后重新封装Cell
                    put.add(cell);
                } else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            }
        }
        context.write(key,put);
    }
}
