package com.adrien.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HBaseDemo {
    //创建Hadoop 以及 HBase管理配置对象
    public static Configuration conf;

    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_POS = "100.168.1.182";
    private static final String ZK_POS = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static final String ZK_PORT_VALUE = "2181";

    static{
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set(ZK_QUORUM, "hadoop103"); // 换成自己的ZK节点IP
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT_VALUE);
    }


    /**
     * 判断表是否已存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExist(String tableName) throws IOException {
        //HBase 中操作表，要创建HBaseAdmin对象
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(String tableName,String... columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        //判断表是否存在
        if (isExist(tableName)) {
            System.out.println("table named" + tableName + "is existed");
        } else {
            //创建表属性对象，表名需要转换字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置 创建表
            admin.createTable(descriptor);
            System.out.println("TABLE " + tableName + "创建成功");
        }

    }

    /**
     * 删除表
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("TABLE " + tableName + "删除成功！！");
        } else {
            System.out.println("TABLE " + tableName + "表不存在");
        }
    }


    /**
     * 向表中插入数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName,String rowkey,String columnFamily,String column,String value) throws IOException {
        //创建HTable对象
        HTable hTable = new HTable(conf, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowkey));
        //向Put对象中组装数据
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
    }


    /**
     * 删除多行数据
     * @param tableName
     * @param rows
     */
    public static void deleteMultiRow(String tableName,String... rows) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        ArrayList<Delete> deletes = new ArrayList<>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deletes.add(delete);
        }
        hTable.delete(deletes);
        hTable.close();
    }


    /**
     * 得到表的所有数据
     * @param tableName
     */
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable 的到 resultScanner 实现类的对象
        ResultScanner scanner = hTable.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //打印 rowKey
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                //打印 column family
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("===============");
            }
        }


    }

    public static void main(String[] args) {
        try {
            //System.out.println(isExist("student1"));
            createTable("fruit_mr","info");
            //dropTable("Daniel001");
//            addRowData("Daniel001","1001","basic_info","name","Nick");
//            addRowData("Daniel001","1001","basic_info","age","23");
//            addRowData("Daniel001","1002","basic_info","name","Jennifer");
//            addRowData("Daniel001","1002","basic_info","age","24");
//            addRowData("Daniel001","1002","job","dept_no","24000");
            //deleteMultiRow("Daniel001","Daniel001");
           // getAllRows("Daniel001");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
