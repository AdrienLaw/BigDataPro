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
import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class FirstHBaseSingleColumnValueFilter {

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
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("f2"),
                    Bytes.toBytes("wife"),
                    CompareFilter.CompareOp.NOT_EQUAL,
                    new SubstringComparator("孙尚香"));
            singleColumnValueFilter.setFilterIfMissing(false);
            scan.setFilter(singleColumnValueFilter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    byte[] family_name = CellUtil.cloneFamily(cell);
                    byte[] qualifier_name = CellUtil.cloneQualifier(cell);
                    byte[] rowkey = CellUtil.cloneRow(cell);
                    byte[] value = CellUtil.cloneValue(cell);
                    //判断id和age字段，这两个字段是整形值
                    if("age".equals(Bytes.toString(qualifier_name))  || "id".equals(Bytes.toString(qualifier_name))){
                        System.out.println("数据的rowkey为 " +  Bytes.toString(rowkey)   +"======数据的列族为 " +
                                Bytes.toString(family_name)+"======数据的列名为 " +  Bytes.toString(qualifier_name) +
                                "==========数据的值为 " +Bytes.toInt(value));
                    }else{
                        System.out.println("数据的rowkey为 " +  Bytes.toString(rowkey)   +"======数据的列族为 " +
                                Bytes.toString(family_name)+"======数据的列名为 " +  Bytes.toString(qualifier_name) +
                                "==========数据的值为 " +Bytes.toString(value));
                    }
                }
            }
        } catch (Exception e) {
        }
    }
}
