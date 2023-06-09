package com.adrien.cdc;

import com.adrien.cdc.common.FlinkCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDCMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        FlinkCatalog.setHiveCatalog(tableEnvironment);
        String taxi_gps_bcdc_drop = "DROP TABLE IF EXISTS taxi_gps_cdc";
        String taxi_gps_bcdc_create =  "create table taxi_gps_cdc" +
                "( taxi_id        STRING,\n" +
                "    gps_dataTime   STRING,\n" +
                "    gps_longitude  STRING,\n" +
                "    gps_latitude   STRING,\n" +
                "    taxi_speed     STRING,\n" +
                "    taxi_direction STRING,\n" +
                "    taxi_status    STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname'='hadoop101',\n" +
                "    'port'='3306',\n" +
                "    'username'='root',\n" +
                "    'password'='up15321370403',\n" +
                "    'database-name'='traffic',\n" +
                "    'table-name'='taxi_gps_mini'\n" +
                ")";
        tableEnvironment.executeSql(taxi_gps_bcdc_drop);
        tableEnvironment.executeSql(taxi_gps_bcdc_create);
        Table table = tableEnvironment.sqlQuery("select * from taxi_gps_cdc limit 5");
        //tableEnvironment.toDataStream(table)
    }
}
