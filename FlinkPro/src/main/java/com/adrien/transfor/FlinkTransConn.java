package com.adrien.transfor;

import com.adrien.datastream.HerMultipartSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class FlinkTransConn {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringStreamSource = enev.fromCollection(Arrays.asList("hello world", "spark flink"));
        DataStreamSource<Integer> integerStreamSource = enev.fromCollection(Arrays.asList(1, 2, 3, 4));
        ConnectedStreams<String, Integer> connectStreamSource = stringStreamSource.connect(integerStreamSource);

        SingleOutputStreamOperator<String> connFlatMap = connectStreamSource.flatMap(new CoFlatMapFunction<String, Integer, String>() {

            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void flatMap2(Integer value, Collector<String> out) throws Exception {
                out.collect(value * 2 + " ");
            }
        });
        connFlatMap.print();
        enev.execute();
    }
}
//12> spark flink
//2> 6
//12> 2
//3> 8
//11> hello world
//1> 4