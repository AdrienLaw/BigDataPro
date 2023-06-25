package com.adrien.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author luohaotian
 */
public class FilePipelineApi {
    public static void main(String[] args) throws Exception {
        //1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);
        //2 读取文件
        String filePath = "src/main/resources/input/clicks.txt";
        tabEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                .field("name", DataTypes.STRING())
                .field("url",DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");
        Table inputTable = tabEnv.from("inputTable");
        Table resultTable = inputTable.select("name,url,temp")
                .filter("name = 'Bob'");
        Table resultSqlTable = tabEnv.sqlQuery("select name,url,temp from inputTable where name ='Bob'");
        tabEnv.toDataStream(resultSqlTable).print();
        //输出到文件
        String outputPath = "src/main/resources/input/out.txt";
        tabEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("url",DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");
        //输出到文件
        resultTable.insertInto("outputTable");
        env.execute();



    }
}
