package com.adrien.timewatermark.time.processtime;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 事件时间有序
 */
public class ProcessTimeOrdered {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = enev.addSource(new SourceFunction<String>() {
            FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //大约 10s 的倍数 发送一个事件
                //获取当前时间转换为字符串
                String currTime = String.valueOf(System.currentTimeMillis());
                //截取后四位
                while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
                    currTime = String.valueOf(System.currentTimeMillis());
                    continue;
                }
                System.out.println("开始发送事件的时间:" + dateFormat.format(System.currentTimeMillis()));
                // 第 13 秒发送两个事件
                TimeUnit.SECONDS.sleep(13);
                // 产生了一个事件，但是由于网络原因，事件没有发送
                ctx.collect("hadoop," + System.currentTimeMillis());
                // 第 16 秒发送一个事件
                ctx.collect("hadoop," + System.currentTimeMillis());
                TimeUnit.SECONDS.sleep(3);
                ctx.collect("hadoop," + System.currentTimeMillis());
                TimeUnit.SECONDS.sleep(300);
            }

            @Override
            public void cancel() {

            }
        })
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String split : splits) {
                            out.collect(new Tuple2<>(split, 1));
                        }
                    }
                })
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                //.timeWindow(Time.seconds(10),Time.seconds(5))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> element : elements) {
                            sum += 1;
                        }
                        out.collect(Tuple2.of(tuple.getField(0), sum));
                    }
                });
        resultStream.print().setParallelism(1);
        enev.execute();
    }
}
