package com.adrien.timewatermark;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 每隔 5s 计算最近 10s 单词出现的次数
 */
public class TimeWindowWordCount {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
//        enev.setParallelism(1);
//        //设置时间类型
//        //enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        enev.addSource(new SourceFunction<String>() {
//            FastDateFormat dateFormat = FastDateFormat.getInstance();
//            @Override
//            public void run(SourceContext<String> ctx) throws Exception {
//                // 控制大约在 10 秒的倍数的时间点发送事件
//                String currentTime = String.valueOf(System.currentTimeMillis());
//                while (Integer.valueOf(currentTime.substring(currentTime.length() - 4)) > 1000) {
//                    currentTime = String.valueOf(System.currentTimeMillis());
//                    continue;
//                }
//                System.out.println("开始发送事件的时间:" + dateFormat.format(System.currentTimeMillis()));
//                // 第 13 秒发送两个事件
//                TimeUnit.SECONDS.sleep(13);
//                ctx.collect("hadoop," + System.currentTimeMillis());
//                // 产生了一个事件，但是由于网络原因，事件没有发送
//                String event = "hadoop," + System.currentTimeMillis();
//                // 第 16 秒发送一个事件
//                TimeUnit.SECONDS.sleep(3);
//                ctx.collect("hadoop," + System.currentTimeMillis());
//                // 第 19 秒的时候发送
//                TimeUnit.SECONDS.sleep(3);
//                ctx.collect(event);
//                TimeUnit.SECONDS.sleep(300);
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        })
//                .map(new MapFunction<String, Tuple2<String,Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(String value) throws Exception {
//                        String[] splits = value.split(",");
//                        return new Tuple2<>(splits[0],Long.valueOf(splits[1]));
//                    }
//                })
//                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
//                    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mmm:ss");
//                    @Override
//                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
//                        return element.f1;
//                    }
//
//                    @Override
//                    public Watermark getCurrentWatermark() {
//                        return new Watermark(System.currentTimeMillis());
//                    }
//                })
//                .keyBy(0)
//                .timeWindow(Time.seconds(10),Time.seconds(5))
//                .process(new ProcessWindowFunction<Tuple2<String, Long>, Tuple2<String,Integer>, Tuple, TimeWindow>() {
//                    //当一个window触发计算的时候会调用这个方法
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        int sum = 0;
//                        for (Tuple2<String, Long> element : elements) {
//                            sum += 1;
//                        }
//                        out.collect(Tuple2.of(tuple.getField(0),sum));
//                    }
//                })
//                .print().setParallelism(1);
//        enev.execute();
//    }

    public static void main(String[] args) {
        Date date = new Date(1461756862000l);
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sd.format(date));
    }
}
