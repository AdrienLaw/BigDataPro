package com.adiren.hbase.filter.filterbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FirstHBasePageFilter2 {
    private static final String ZK_CONNECT_ADDR = "hadoop103:2181,hadoop104:2181,hadoop105:2181";
    private static final String TABLE_NAME = "myuser";
    private static final String FAMILY_F1 = "f1";
    private static final String FAMILY_F2 = "f2";
    private static final String COLUMN_ADDR = "address";
    private static final String COLUMN_NAME = "name";
    private static final String ROW_KEY = "0003";

    private static Configuration configuration = null;
    private static Connection connection = null;
    private static Table table = null;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",ZK_CONNECT_ADDR);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
        } catch (Exception e) {
        }
    }

    public static ResultScanner getPageData (int pageIndex , int pageNumber) throws Exception {
        //
        String startRow = null;
        if (pageIndex == 1) {
            ResultScanner pageDataScanner = getPageData(startRow, pageNumber);
            return pageDataScanner;
        } else {

        }
        return null;
    }

    public static ResultScanner getPageData (String startRow, int pageNumber) throws Exception {
        Scan scan = new Scan();
        //param 1 family ; param 2 qualifier
        scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("address"));
        //设置当前查询起始位置
        if (!StringUtils.isBlank(startRow)) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        //第二个参数
        PageFilter pageFilter = new PageFilter(pageNumber);
        scan.setFilter(pageFilter);
        ResultScanner scanner = table.getScanner(scan);
        return scanner;
    }
}
