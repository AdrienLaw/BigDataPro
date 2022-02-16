package com.adiren.hbase.ddml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class FristHBaseGetData {
    private Connection connection ;
    private final String TABLE_NAME = "myuser";
    private Table table ;

    public static void main(String[] args) {
        Connection connection = null;
        final String TABLE_NAME = "myuser";
        Table table = null;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop103:2181,hadoop104:2181,hadoop105:2181");
        //通过get对象，指定rowkey
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(Bytes.toBytes("0005"));
            get.addFamily("f2".getBytes());//限制只查询f1列族下面所有列的值
            //查询f2  列族 phone  这个字段
            get.addColumn("f2".getBytes(),"phone".getBytes());
            //通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面了
            Result result = table.get(get);
            List<Cell> cells = result.listCells();  //获取一条数据所有的cell，所有数据值都是在cell里面 的
            if(cells != null) {
                for (Cell cell : cells) {
                    byte[] family_name = CellUtil.cloneFamily(cell);//获取列族名
                    byte[] column_name = CellUtil.cloneQualifier(cell);//获取列名
                    byte[] rowkey = CellUtil.cloneRow(cell);//获取rowkey
                    byte[] cell_value = CellUtil.cloneValue(cell);//获取cell值
                    //需要判断字段的数据类型，使用对应的转换的方法，才能够获取到值
                    if("age".equals(Bytes.toString(column_name))  || "id".equals(Bytes.toString(column_name))){
                        System.out.println(Bytes.toString(family_name));
                        System.out.println(Bytes.toString(column_name));
                        System.out.println(Bytes.toString(rowkey));
                        System.out.println(Bytes.toInt(cell_value));
                    }else{
                        System.out.println(Bytes.toString(family_name));
                        System.out.println(Bytes.toString(column_name));
                        System.out.println(Bytes.toString(rowkey));
                        System.out.println(Bytes.toString(cell_value));
                    }
                }
                //table.close();
            }
        } catch (Exception e) {

        }
    }
}
