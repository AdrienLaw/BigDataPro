package com.adrien.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestCountWindowWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        enev.setParallelism(1);
        DataStreamSource<String> socketTextStream = enev.socketTextStream("hadoop101", 9909);
        SingleOutputStreamOperator<Tuple2<String, Integer>> keyedStream = socketTextStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                    String[] splits = value.split(" ");
                    for (String split : splits) {
                        out.collect(new Tuple2<>(split, 1));
                    }
                }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowOperat = keyedStream
                .keyBy(0)
                //滚动窗口 每次窗口中出现了10个相同的元素就计算结果。
                //.countWindow(10)
                //滑动窗口 每收到 10 个相同 key 的数据就计算一次，每一次计算的 window 范围是 20 个元素。
                .countWindow(20,10)
                .sum(1);
        windowOperat.print();
        enev.execute("TestTimeWindow");
    }
}
