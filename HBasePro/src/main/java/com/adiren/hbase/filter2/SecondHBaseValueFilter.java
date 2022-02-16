package com.adiren.hbase.filter2;

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
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class SecondHBaseValueFilter {


    public static void main(String[] args) {
        Connection connection = null;
        final String TABLE_NAME = "myuser";
        Table table = null;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Scan scan = new Scan();
            SubstringComparator substringComparator = new SubstringComparator("name");
            QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, substringComparator);
            scan.setFilter(qualifierFilter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] qualifier = CellUtil.cloneQualifier(cell);
                    byte[] rowKey = CellUtil.cloneRow(cell);
                    byte[] value = CellUtil.cloneValue(cell);
                    if ("age".equals(Bytes.toString(qualifier))  || "id".equals(Bytes.toString(qualifier))) {
                    //判断id和age 字段是否为整if("age".equals(Bytes.toString(qualifier)) || "id".equals(Bytes.toString(qualifier))) {
                        System.out.println("数据的rowkey为** " +  Bytes.toString(rowKey)   +"======数据的列族为 " +
                                Bytes.toString(family)+"======数据的列名为 " +  Bytes.toString(qualifier) +
                                "==========数据的值为 " +Bytes.toInt(value));
                    } else {
                        System.out.println("数据的rowkey为** " +  Bytes.toString(rowKey)   +"======数据的列族为 " +
                                Bytes.toString(family)+"======数据的列名为 " +  Bytes.toString(qualifier) +
                                "==========数据的值为 " +Bytes.toInt(value));
                    }
                }
            }
        } catch (Exception e) {

        }

    }
}
