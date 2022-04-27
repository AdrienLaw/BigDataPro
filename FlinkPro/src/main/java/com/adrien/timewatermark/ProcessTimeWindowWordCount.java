package com.adrien.timewatermark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 我想删掉你
 */
public class ProcessTimeWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.setParallelism(1);
        DataStreamSource<String> socketTextStream = enev.socketTextStream("hadoop101", 9909);
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.split(" ");
                for (String split : splits) {
                    out.collect(new Tuple2<>(split,1));
                }
            }
        }).keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context,
                                        Iterable<Tuple2<String, Integer>> elements,
                                        Collector<Tuple2<String, Integer>> out) throws Exception {
                        System.out.println("当前系统时间为："+  System.currentTimeMillis());
                        System.out.println("window的处理时间为："+ context.currentProcessingTime());
                        System.out.println("window的开始时间为："+ context.window().getStart());
                        System.out.println(" window的结束时间为："+ context.window().getEnd());

                        int sum = 0;
                        for (Tuple2<String, Integer> element : elements) {
                            sum += element.f1;
                        }
                        out.collect(new Tuple2<>(tuple.getField(0),sum));
                    }
                }).print();
        enev.execute();
    }
}
