package com.adiren.hbase.filter.comparefilter;

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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class FristHBaseRowFilter {

    public static void main(String[] args) {
        Connection connection = null;
        final String TABLE_NAME = "myuser";
        Table table = null;
        //初始化
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop103:2181,hadoop104:2181,hadoop105:2181");
        Scan scan = new Scan();
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            /***
             * rowFilter需要加上两个参数
             * 第一个参数就是我们的比较规则
             * 第二个参数就是我们的比较对象
             */
            BinaryComparator binaryComparator = new BinaryComparator("0003".getBytes());
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, binaryComparator);
            scan.setFilter(rowFilter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    byte[] family_name = CellUtil.cloneFamily(cell);
                    byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                    byte[] rowkey = CellUtil.cloneRow(cell);
                    byte[] value = CellUtil.cloneValue(cell);
                    //判断id和age字段，这两个字段是整形值
                    if ("age".equals(Bytes.toString(qualifier_name)) || "id".equals(Bytes.toString(qualifier_name))) {
                        System.out.println("数据的rowkey为 " + Bytes.toString(rowkey) + "======数据的列族为 " + Bytes.toString(family_name) +
                                "======数据的列名为 " + Bytes.toString(qualifier_name) + "==========数据的值为 " + Bytes.toInt(value));
                    } else {
                        System.out.println("数据的rowkey为 " + Bytes.toString(rowkey) + "======数据的列族为 " + Bytes.toString(family_name) +
                                "======数据的列名为 " + Bytes.toString(qualifier_name) + "==========数据的值为 " + Bytes.toString(value));
                    }
                }
            }
        } catch (Exception e) {
        }
    }
}
