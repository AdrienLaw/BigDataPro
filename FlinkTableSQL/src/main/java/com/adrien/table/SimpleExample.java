package com.adrien.table;


import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;

/**
 * @author luohaotian
 */
public class SimpleExample {

    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(enev);
        //create datastream
        DataStream<Row> dataStream = enev.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        Table outputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        tableEnv.createTemporaryView("InputTable", inputTable);
        tableEnv.createTemporaryView("OutputTable", outputTable);

        String insertQuery = "INSERT INTO OutputTable SELECT * FROM InputTable";
        tableEnv.executeSql(insertQuery);
        Table resultTable = tableEnv.sqlQuery(
                "SELECT name, SUM(score) FROM OutputTable GROUP BY name");
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        // add a printing sink and execute in DataStream API
        resultStream.print();
        enev.execute();

//
//        // execute with explicit sink
//        tableEnv.from("InputTable").executeInsert("OutputTable");
//        tableEnv.createTemporaryView("InputTable", inputTable);
//        tableEnv.executeSql("INSERT INTO OutputTable SELECT * FROM InputTable");
//        tableEnv.createStatementSet()
//                .addInsert("OutputTable", tableEnv.from("InputTable"))
//                .execute();
//        tableEnv.createStatementSet()
//                .addInsertSql("INSERT INTO OutputTable SELECT * FROM InputTable")
//                .execute();
//        // execute with implicit local sink
//
//        tableEnv.from("OutputTable").execute().print();
//        tableEnv.executeSql("SELECT * FROM OutputTable").print();
    }



    public void mostSingleTwo (String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(enev);
        //create datastream
        DataStream<Row> dataStream = enev.fromElements(
                Row.of("Alice", 12),
                Row.of("Bob", 10),
                Row.of("Alice", 100));
        //dataStream -> table
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
        tableEnv.createTemporaryView("InputTable", inputTable);

        Table resultTable = tableEnv.sqlQuery(
                "SELECT name, SUM(score) FROM InputTable GROUP BY name");

        // interpret the updating Table as a changelog DataStream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
        // add a printing sink and execute in DataStream API
        resultStream.print();
        enev.execute();
        //      9> +I[Alice, 12]
        //      8> +I[Bob, 10]
        //      9> -U[Alice, 12]
        //      9> +U[Alice, 112]
    }


    public void mostSingleOne(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);
        // create a DataStream
        DataStream<String> dataStream = env.fromElements("Alice", "Jennifer", "Daniel");
        // interpret the insert-only DataStream as a Table
        Table table = tabEnv.fromDataStream(dataStream);
        tabEnv.createTemporaryView("inouTAB",table);
        Table sqlQuery = tabEnv.sqlQuery("SELECT UPPER(f0) FROM inouTAB");
        DataStream<Row> rowDataStream = tabEnv.toDataStream(sqlQuery);
        rowDataStream.print();
        env.execute();
        //    11> +I[JENNIFER]
        //    10> +I[ALICE]
        //    12> +I[DANIEL]
    }

    public void envConfig () {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set various configuration early
        env.setMaxParallelism(256);
    /*  env.getConfig().addDefaultKryoSerializer(MyCustomType.class, CustomKryoSerializer.class); */
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // then switch to Java Table API
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // set configuration early

        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));
        // start defining your pipelines in both APIs...
    }

}
