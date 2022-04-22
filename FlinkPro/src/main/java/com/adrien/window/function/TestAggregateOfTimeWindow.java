package com.adrien.window.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TestAggregateOfTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = enev.socketTextStream("hadoop101", 9909);
        DataStreamSink<Tuple2<String, Integer>> dataStreamSink = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits) {
                    out.collect(new Tuple2<>(split, 1));
                }
            }
        }).keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new HerAggregateFunction())
                .print();
        enev.execute();
        //spark spark spark
        //  1> (spark,3)
        //kafka spark
        //  1> (kafka,1)
        //  1> (spark,1)
    }

}
