package com.adrien.window;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
/**
 * 基于 Flink 流计算实时词频统计WordCount, 进行窗口Window内数据统计
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 执行环境-env：流计算执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1) ;
        // 2. 数据源-source：Socket接收数据
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop101", 9909);
        // 3. 转换处理-transformation：调用DataSet函数，处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleStream = inputStream
                // a. 过滤数据
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        return null != line && line.trim().length() > 0;
                    }
                })
                // b. 分割单词
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        String[] words = line.trim().toLowerCase().split("\\W+");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                // c. 转换二元组，表示每个单词出现一次
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                });
        // TODO： 针对KeyedDataStream进行时间窗口聚合操作，先分组，后窗口，再聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowCountStream = tupleStream
        // 按照单词分组，下标索引为0
                .keyBy(0)
        // TODO: 设置时间窗口大小为3秒
                //.timeWindow(Time.seconds(3))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .sum(1);
        windowCountStream.print("keyByWindow");
        // TODO： 针对非KeyedDataStream进行时间窗口聚合操作，先窗口，再聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> windowStream = tupleStream
        // TODO: 设置时间窗口大小为3秒
                //.timeWindowAll(Time.seconds(3))
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .sum(1);
        windowStream.printToErr("windowAll");
        // 5. 触发执行-execute
        env.execute(WindowWordCount.class.getSimpleName()) ;
    }
}