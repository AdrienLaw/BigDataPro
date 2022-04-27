package com.adrien.timewatermark.watermark;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 3秒 统计一波 前 3 秒相同 Key 的事件
 */
public class WaterMarkWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment exev = StreamExecutionEnvironment.getExecutionEnvironment();
        exev.setParallelism(1);
        exev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水位线产生 周期为 1s
        exev.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<String> sourceStream = exev.socketTextStream("hadoop101", 9909);
        sourceStream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Tuple2<>(splits[0],Long.valueOf(splits[1]));
            }
        })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
                    private long currentMaxEventTime = 0L;
                    private long maxOutOfOrderness = 1000L;
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        //当前 事件时间
                        long currentElementEventTime = element.f1;
                        // 谁时间大？
                        currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);
                        System.out.println("event = " + element
                                        + "|" + dateFormat.format(element.f1) // Event Time
                                        + "|" + dateFormat.format(currentMaxEventTime) // Max EventTime
                                        + "|" + dateFormat.format(getCurrentWatermark().getTimestamp())); // Current Watermar
                        return currentElementEventTime;
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        //System.out.println("water .. mark ...");
                        return new Watermark(currentMaxEventTime - maxOutOfOrderness);
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        System.out.println("处理时间:" +
                                dateFormat.format(context.currentProcessingTime())); System.out.println("window start time : " +
                                dateFormat.format(context.window().getStart()));
                        List<String> list = new ArrayList<>();
                        for (Tuple2<String, Long> element : elements) {
                            list.add(element.toString() + "|" + dateFormat.format(element.f1));
                        }
                        out.collect(list.toString());
                        //System.out.println("window end time : " + dateFormat.format(context.window().getEnd()));
                    }
                }).print().setParallelism(1);
        exev.execute();
    }
}
