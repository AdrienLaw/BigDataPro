package com.adrien.timewatermark;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;


public class OrderedStreamWaterMark {
    public static void main(String[] args) throws Exception {
        //设置环境
        StreamExecutionEnvironment exev = StreamExecutionEnvironment.getExecutionEnvironment();
        //2 设置时间类型   以事件发生时间为准去计算
        exev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSink<Tuple2<String, Integer>> stringDataStreamSource = exev.addSource(new SourceFunction<String>() {
            FastDateFormat dateFormat = FastDateFormat.getInstance("HH:MM:SS");
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                // 控制大约在 10 秒的倍数的时间点发送事件
                String currentTime = String.valueOf(System.currentTimeMillis());
                while (Integer.valueOf(currentTime.substring(currentTime.length() - 4)) > 100) {
                    currentTime = String.valueOf(System.currentTimeMillis());
                    continue;
                }
                System.out.println("==== 开始发送事件时间 =====" + dateFormat.format(System.currentTimeMillis()));
                //第 13 秒 发送两个事件
                TimeUnit.SECONDS.sleep(13);
                ctx.collect("hadoop," + System.currentTimeMillis());
                //产生了一个时事件
                String event = "hadoop," + System.currentTimeMillis();
                //第16秒 发送一个事件
                TimeUnit.SECONDS.sleep(3);
                ctx.collect("hadoop," + System.currentTimeMillis());
                //19秒
                TimeUnit.SECONDS.sleep(3);
                ctx.collect(event);
                TimeUnit.SECONDS.sleep(300);
            }

            @Override
            public void cancel() {
            }
        }).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Tuple2<>(splits[0], Long.valueOf(splits[1]));
            }
        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:MM:SS");

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        //window 延迟5s 触发
                        return new Watermark(System.currentTimeMillis() - 5000);
                    }
        }).keyBy(0).timeWindow(Time.seconds(10),Time.seconds(5))
        .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
                    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
                    /**
                     * 当一个window触发计算的时候会调用这个方法 * @param tuple key
                     */
                    @Override
                    public void process(Tuple tuple, Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Long> element : elements) {
                            sum += 1;
                        }
                        out.collect(Tuple2.of(tuple.getField(0), sum));
                    }
                }).print().setParallelism(1);
        exev.execute();
    }

}
