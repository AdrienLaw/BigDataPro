package com.adiren.hbase.filter.filterbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class FirstHBasePageFilter {
    public static void main(String[] args) {
        Connection connection = null;
        final String TABLE_NAME = "myuser";
        Table table = null;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop103:2181,hadoop104:2181,hadoop105:2181");
        //通过get对象，指定rowkey
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Scan scan = new Scan();
            int pageNum = 3;
            int pageSize = 2;
            if (pageNum == 1) {
                scan.setStartRow("".getBytes());
                PageFilter pageFilter = new PageFilter(pageSize);
                scan.setFilter(pageFilter);
            } else {
                String startRow = "";
                //扫描 5 条数据
                int scanDatas = (pageNum - 1) * pageSize + 1;
                PageFilter pageFilter = new PageFilter(pageSize);
                scan.setFilter(pageFilter);
                ResultScanner scanner = table.getScanner(scan);
                for (Result result : scanner) {
                    byte[] row = result.getRow();//获取rowKey
                    //------
                }
            }

        } catch (Exception e) {
        }
    }
}
