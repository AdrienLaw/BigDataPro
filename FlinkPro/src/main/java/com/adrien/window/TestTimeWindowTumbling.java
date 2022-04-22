package com.adrien.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestTimeWindowTumbling {
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
                //.window(TumblingEventTimeWindows.of(Time.seconds(5))) api 过期
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) 这个行
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(15),Time.seconds(5)))
                .countWindow(5,2)
                .sum(1);

        windowOperat.print();
        enev.execute("TestTimeWindowTumbling");

    }
}
