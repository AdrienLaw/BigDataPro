package com.adrien.operateState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 每两条数据打印一次结果 1000
 */
public class TestOperatorListStae {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment exev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = exev.fromElements(
                Tuple2.of("Spark", 3), Tuple2.of("Hadoop", 5),
                Tuple2.of("Hadoop", 7), Tuple2.of("Spark", 4));
        dataStreamSource.addSink(new OperatorListStateSink(2)).setParallelism(1);
        exev.execute();
    }
}
