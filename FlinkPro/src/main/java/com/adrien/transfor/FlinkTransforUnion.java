package com.adrien.transfor;

import com.adrien.datastream.HerMultipartSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkTransforUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> text1 = enev.addSource(new HerMultipartSource()).setParallelism(1);
        DataStreamSource<Long> text2 = enev.addSource(new HerMultipartSource()).setParallelism(1);
        DataStream<Long> unionDataStream = text1.union(text2);
        SingleOutputStreamOperator<Long> transforUnion = unionDataStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("==== 原始数据 =========" + value);
                return value;
            }
        });
        transforUnion.print();
        enev.execute();
        //==== 原始数据 =========2
        //==== 原始数据 =========2
        //3> 2
        //11> 2
        //==== 原始数据 =========3
        //4> 3
        //==== 原始数据 =========3
        //12> 3
    }
}
