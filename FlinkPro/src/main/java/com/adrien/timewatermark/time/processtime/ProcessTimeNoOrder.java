package com.adrien.timewatermark.time.processtime;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * process time 面对无序
 */
public class ProcessTimeNoOrder {
    public static void main(String[] args) throws Exception {
        //设置环境
        StreamExecutionEnvironment exev = StreamExecutionEnvironment.getExecutionEnvironment();
        //2 设置时间类型   以事件发生时间为准去计算
        exev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSink<Tuple2<String, Integer>> stringDataStreamSource = exev.addSource(new SourceFunction<String>() {
            FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss"); @Override
            public void run(SourceContext<String> ctx) throws Exception {
                // 控制大约在 10 秒的倍数的时间点发送事件
                String currTime = String.valueOf(System.currentTimeMillis());
                while (Integer.valueOf(currTime.substring(currTime.length() - 4)) >100) {
                        currTime = String.valueOf(System.currentTimeMillis());
                    continue;
                }
                System.out.println("开始发送事件的时间:" + dateFormat.format(System.currentTimeMillis()));
                // 第 13 秒发送两个事件
                TimeUnit.SECONDS.sleep(3);
                ctx.collect("hadoop," + System.currentTimeMillis());
                // 产生了一个事件，但是由于网络原因，事件没有发送
                String event = "hadoop," + System.currentTimeMillis();
                // 第 16 秒发送一个事件
                TimeUnit.SECONDS.sleep(3);
                ctx.collect("hadoop," + System.currentTimeMillis());
                // 第 19 秒的时候发送
                TimeUnit.SECONDS.sleep(3);
                ctx.collect(event);
                TimeUnit.SECONDS.sleep(300);
            }
            @Override
            public void cancel() {
            }
        })
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(new Tuple2<>(split,1));
                }
            }
        })
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> ele : elements) {
                            sum += 1;
                        }
                        // 输出单词出现的次数
                        out.collect(Tuple2.of(tuple.getField(0), sum));
                    }
                }).print().setParallelism(1);
        exev.execute();
    }
}
