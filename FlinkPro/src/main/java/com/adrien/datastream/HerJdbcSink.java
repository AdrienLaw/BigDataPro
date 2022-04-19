package com.adrien.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class HerJdbcSink extends RichSinkFunction<SensorReading> {

    Connection conn = null;
    PreparedStatement insertStmt = null;
    PreparedStatement updateStmt = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection("jdbc:mysql://hadoop101:3306", "root", "123456");
        insertStmt = conn.prepareStatement("INSERT INTO sensor (id, temp) VALUES (?, ?)");
        updateStmt = conn.prepareStatement("UPDATE sensor SET temp = ? WHERE id = ?");
    }

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        // 执行更新语句，注意不要留 super
        updateStmt.setDouble(1, value.getTemperature());
        updateStmt.setString(2, value.getId());
        updateStmt.execute();
        // 如果刚才 update 语句没有更新，那么插入
        if (updateStmt.getUpdateCount() == 0) {
            insertStmt.setString(1, value.getId());
            insertStmt.setDouble(2, value.getTemperature());
            insertStmt.execute();
        }
    }

    @Override
    public void close() throws Exception {
        insertStmt.close();
        updateStmt.close();
        conn.close();
    }

}
