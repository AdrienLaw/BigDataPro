package com.adrien.window.function;

import org.apache.flink.api.common.functions.AggregateFunction;
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
        DataStreamSink<Tuple2<String, Integer>> dataStreamSink = socketTextStream.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.split(" ");
                        out.collect(new Tuple2<>(splits[0],Integer.valueOf(splits[1])));
                    }
        })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {

                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return  new Tuple2<String,Integer>("",0);
                    }

                    @Override
                    public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                        return new Tuple2<String,Integer>(value.f0,value.f1 + accumulator.f1);
                    }

                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return new Tuple2<String,Integer>(a.f0,a.f1 + b.f1);
                    }
                }).print();
        enev.execute();
    }

}
