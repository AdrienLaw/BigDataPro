package com.adiren.hbase.filter.expandFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class FirstHBaseSkipFilter {
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
            //设置首次行键过滤器
            ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("曹操")));
            scan.setFilter(valueFilter);
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
            scanner.close();
            System.out.println("============== 应用SkipFilter =================");
            //应用SkipFilter
            SkipFilter skipFilter = new SkipFilter(valueFilter);
            Scan scanSkip = new Scan();
            scanSkip.setFilter(skipFilter);
            ResultScanner scannerSkip = table.getScanner(scanSkip);
            for (Result result : scannerSkip) {
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
            scannerSkip.close();
        } catch (Exception e) {
        }
    }
}
