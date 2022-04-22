package com.adrien.window.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestApplyOfTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment exev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = exev.socketTextStream("hadoop101", 9909);
        socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        }).map(
                new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>("countAvg",Integer.valueOf(value));
                    }
                }
        )
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Double, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window,
                                      Iterable<Tuple2<String, Integer>> input,
                                      Collector<Double> out) throws Exception {
                        Double totalNum = 0D;
                        Double countNum = 0D;
                        for (Tuple2<String, Integer> stringIntegerTuple2 : input) {
                            totalNum++;
                            countNum = countNum + stringIntegerTuple2.f1;
                        }
                        Double v = countNum / totalNum;
                        out.collect(v);
                    }
                }).print();

        exev.execute();
    }
}
