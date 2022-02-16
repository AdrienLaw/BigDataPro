package com.adiren.hbase.ddml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class FirstHBaseAddData {
    private Connection connection ;
    private final String TABLE_NAME = "myuser";
    private Table table ;

    public static void main(String[] args) {
        Connection connection = null;
        final String TABLE_NAME = "myuser";
        Table table = null;
        //初始化
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            //获取表
            //Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Put put = new Put("0001".getBytes());//创建put对象，并指定rowkey值
            put.addColumn("f1".getBytes(),"name".getBytes(),"zhangsan".getBytes());
            put.addColumn("f1".getBytes(),"age".getBytes(), Bytes.toBytes(18));
            put.addColumn("f1".getBytes(),"id".getBytes(), Bytes.toBytes(25));
            put.addColumn("f1".getBytes(),"address".getBytes(), Bytes.toBytes("地球人"));
            table.close();
            connection.close();
        } catch (Exception e) {

        }
    }
}
