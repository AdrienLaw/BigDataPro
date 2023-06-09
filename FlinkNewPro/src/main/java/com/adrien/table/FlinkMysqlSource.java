package com.adrien.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class FlinkMysqlSource {
    public static void main(String[] args) throws Exception {
        //1、创建TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.
                newInstance().
                inStreamingMode().
                useBlinkPlanner().
                build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //2、创建Mysql source table
         String table_sql =
                "CREATE TABLE taxi_gps_bin_flink " +
                        "( taxi_id        STRING,\n" +
                        "    gps_dataTime   STRING,\n" +
                        "    gps_longitude  STRING,\n" +
                        "    gps_latitude   STRING,\n" +
                        "    taxi_speed     STRING,\n" +
                        "    taxi_direction STRING,\n" +
                        "    taxi_status    STRING\n" +
                        ")  WITH (\n" +
                        "  'connector.type' = 'jdbc',\n" +
                        "  'connector.url' = 'jdbc:mysql://hadoop101:3306/traffic?useUnicode=false&characterEncoding=utf-8', \n" +
                        "  'connector.driver' = 'com.mysql.jdbc.Driver', \n" +
                        "  'connector.table' = 'taxi_gps_mini', \n" +
                        "  'connector.username' = 'root',\n" +
                        "  'connector.password' = 'up15321370403', \n" +
                        "  'connector.read.fetch-size' = '3' \n" +
                        ")";
        tableEnv.executeSql("DROP TABLE IF EXISTS taxi_gps_bin_flink");
        tableEnv.executeSql(table_sql);
        String sql = "select * from taxi_gps_bin_flink";
        Table table = tableEnv.sqlQuery(sql);
        table.printSchema();
        //table 转成 dataStream 流
        DataStream<Row> behaviorStream = tableEnv.toAppendStream(table, Row.class);
        behaviorStream.flatMap(new FlatMapFunction<Row, Object>() {
            @Override
            public void flatMap(Row value, Collector<Object> out) throws Exception {
                System.out.println(value.toString());
                Thread.sleep(1 * 1000);
            }
        }).print();
        env.execute();
    }
}
