package com.adrien.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FirstFlinkWordCount {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        enev.setParallelism(4);
        DataStreamSource<String> dataStream = enev.socketTextStream("hadoop101", 9909);
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = line.split(" ");
                for (String word : split) {
                    collector.collect(new Tuple2<>(word, 1));
                }

            }
        }).keyBy(0).sum(1).setParallelism(2);
        streamOperator.print();
        enev.execute();
    }
}
