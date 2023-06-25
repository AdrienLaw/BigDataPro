package com.adrien.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author luohaotian
 */
public class FlinkTableConnectKafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 连接Kafka，读取数据
        tableEnv.executeSql("CREATE TABLE inputTable" +
                "(`name` string," +
                " `url` string," +
                "`temp` double)" +
                "with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'sensor'," +
                "'properties.zookeeper.connect' = '39.107.94.247:2181,101.200.189.84:2181,112.126.72.2:2181'," +
                "'properties.bootstrap.servers' = '182.92.209.26:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'format' = 'csv'," +
                "'csv.field-delimiter' = ','" +
                ")");

        // 3. 查询转换
        Table resultTable = tableEnv.sqlQuery("select name,url,temp from inputTable where name = 'Bob'");
        //tableEnv.toDataStream(resultTable).print();

        // 4. 建立kafka连接，输出到不同的topic下
        tableEnv.executeSql("CREATE TABLE outputTable" +
                "(`name` string," +
                " `url` string," +
                "`temp` double)" +
                "with (" +
                "'connector' = 'kafka'," +
                "'topic' = 'sinkSenor'," +
                "'properties.zookeeper.connect' = '39.107.94.247:2181,101.200.189.84:2181,112.126.72.2:2181'," +
                "'properties.bootstrap.servers' = '182.92.209.26:9092,39.107.94.247:9092'," +
                "'properties.group.id' = 'testGroup'," +
                "'format' = 'csv'," +
                "'csv.field-delimiter' = ','" +
                ")");

        resultTable.executeInsert("outputTable");
        //env.execute();

//[root@hadoop102 kafka_2.12-2.6.0]# bin/kafka-topics.sh --zookeeper hadoop103:2181 --list
//        __consumer_offsets
//        csdn_wyk
//[root@hadoop102 kafka_2.12-2.6.0]# bin/kafka-topics.sh --zookeeper hadoop103:2181 --create --topic sensor --partitions 3 --replication-factor 3
//        Created topic sensor.
//[root@hadoop102 kafka_2.12-2.6.0]# bin/kafka-topics.sh --zookeeper hadoop103:2181 --create --topic sinkSenor --partitions 3 --replication-factor 3
//Created topic sinkSenor.
//[root@hadoop102 kafka_2.12-2.6.0]#
    }
}
