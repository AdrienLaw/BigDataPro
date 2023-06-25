package com.adrien.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

public class FlinkHive {
    public static void main(String[] args) {
        //TODO 1 设置执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode() // 有流和批inBatchMode() 任选
                .build();

        //TODO 2 表执行环境
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //TODO 3 定义Hive配置
        String name = "myHive"; // HiveCatalog 名称 唯一表示 随便起
        String defaultDatabase = "testsort"; // 默认数据库名称，连接之后默认的数据库
        String hiveConfDir = "conf/";  //hive-site.xml存放的位置，本地写本地，集群写集群的路径

        //TODO 4 注册
//        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir);
//        tableEnv.registerCatalog(name,hiveCatalog);

        //TODO 5 操作
        tableEnv.useCatalog(name); // 使用这个catalog
        tableEnv.useDatabase(defaultDatabase);// 要操作的数据库

        //TODO 6 查询
        Table table = tableEnv.sqlQuery("select * from action");// 动态表
        TableResult result = table.execute(); // 执行查询，获取结果
        result.print();// 打印结果
        //tableEnv.executeSql("insert into t2 select id,name from t1"); // 执行插入

    }
}
