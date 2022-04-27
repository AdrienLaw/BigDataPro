package com.adrien.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TestTimeWindowWC {
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowOperat = keyedStream.keyBy(0)
                //每隔 5s 统计最近 5s 出现的单词个数
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //每隔 5s 统计最近 15s 出现单词个数
                .window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)))
                .sum(1);

        windowOperat.print();
        enev.execute("TestTimeWindow");
    }
}
