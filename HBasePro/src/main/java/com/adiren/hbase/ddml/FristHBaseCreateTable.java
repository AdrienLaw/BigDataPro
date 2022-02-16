package com.adiren.hbase.ddml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class FristHBaseCreateTable {
    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        //连接HBase集群不需要指定HBase主节点的ip地址和端口号
        configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
        //创建连接对象
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            //获取连接对象，创建一张表
            //获取管理员对象，来对手数据库进行DDL的操作
            Admin admin = connection.getAdmin();
            //指定我们的表名
            TableName myuser = TableName.valueOf("myuser");
            HTableDescriptor hTableDescriptor = new HTableDescriptor(myuser);
            //指定两个列族
            HColumnDescriptor f1 = new HColumnDescriptor("f1");
            HColumnDescriptor f2 = new HColumnDescriptor("f2");
            hTableDescriptor.addFamily(f1);
            hTableDescriptor.addFamily(f2);
            admin.createTable(hTableDescriptor);
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
