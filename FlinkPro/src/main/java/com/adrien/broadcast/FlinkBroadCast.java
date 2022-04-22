package com.adrien.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import scala.Int;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FlinkBroadCast {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment enev = ExecutionEnvironment.getExecutionEnvironment();
        //准备一个数据集
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        //要广播的数据
        DataSource<Tuple2<String, Integer>> tupleSource = enev.fromCollection(broadData);
        //map 一下需要广播的数据 将其转化为 Map
        MapOperator<Tuple2<String, Integer>, HashMap<String, Integer>> toBroadStream = tupleSource.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> res = new HashMap<>();
                res.put(value.f0, value.f1);
                return res;
            }
        });

        //正经的数据
        DataSource<String> dataSource = enev.fromElements("zs", "ls", "ww");
        //获取广播变量
        DataSet<String> resultDataStream = dataSource.map(new RichMapFunction<String, String>() {

            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //获取广播变量
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadcastMapName");
                for (HashMap<String, Integer> map : broadCastMap) {
                    allMap.putAll(map);
                }

            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," +  age;
            }
        }).withBroadcastSet(toBroadStream, "broadCastMapName");
        resultDataStream.print();
        enev.execute();
    }
}
