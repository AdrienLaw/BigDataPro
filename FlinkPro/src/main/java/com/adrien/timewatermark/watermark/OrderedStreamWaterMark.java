package com.adrien.timewatermark.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderedStreamWaterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> socketSourceStream = enev.socketTextStream("hadoop101", 9909);
        SingleOutputStreamOperator<Tuple2<String, Long>> mapOperator = socketSourceStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Tuple2<>(splits[0], Long.valueOf(splits[1]));
            }
        });
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String,Long>>forMonotonousTimestamps().withTimestampAssigner(
//                new SerializableTimestampAssigner<Tuple2<String,Long>>() {
//                    @Override
//                    public long extractTimestamp(Tuple2<String,Long> element, long recordTimestamp) {
//                        return element.f1;
//                    }
//                }
//        )).keyBy(0)
//         .timeWindow(Time.seconds(5))
//         .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String,Integer>, Tuple, TimeWindow>() {
//                @Override
//                public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
//                    String field = tuple.getField(0);
//                    long windowEndTime = context.window().getEnd();
//                    long windowStartTime = context.window().getStart();
//                    long watermark = context.currentWatermark();
//
//                    int sum = 0;
//                    List<Tuple2<String, Long>> tuple2s = Lists.newArrayList(elements);
//                    for (Tuple2<String, Long> tuple2 : tuple2s) {
//                        sum +=1;
//                    }
//
//                    System.out.println("窗口的数据条数:"+sum+
//                            " |窗口的第一条数据：" + tuple2s.get(0)+
//                            " |窗口的最后一条数据：" + tuple2s.get(tuple2s.size()-1) +
//                            " |窗口的开始时间： " + windowStartTime+
//                            " |窗口的结束时间： " + windowEndTime+
//                            " |当前的watermark:"+ watermark);
//                    out.collect(new Tuple2<>(field,sum));
//                }
//         }).print();
        enev.execute();
    }
}
