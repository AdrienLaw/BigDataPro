package com.adrien.operateState;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class TestBroadcastState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSourceStream = enev.socketTextStream("hadoop101", 9909);
        DataStreamSource<String> ctrlSourceStream = enev.socketTextStream("hadoop101", 9910);
        // 解析控制流中的数据成二元组
        SingleOutputStreamOperator<Tuple2<String, String>> ctrlStream = ctrlSourceStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] string = value.split(" ");
                return Tuple2.of(string[0], (string[1]));
            }
        });
        //broadcast 控制流里的数据
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>(
                "controlStream",
                String.class,
                String.class);
        BroadcastStream<Tuple2<String, String>> broadcastStream = ctrlStream.broadcast(mapStateDescriptor);

        DataStreamSink<String> controlStream = dataSourceStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, Tuple2<String, String>, String>() {
                    MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>(
                            "controlStream",
                            String.class,
                            String.class);

                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        String keywords = ctx.getBroadcastState(mapStateDescriptor).get("key");
                        if (value.contains(keywords)) {
                            out.collect(value);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                        // 将接收到的控制数据放到 broadcast state 中
                        ctx.getBroadcastState(mapStateDescriptor).put(value.f0, value.f1);
                        //打印控制信息
                        System.out.println(Thread.currentThread().getName() + " 接收到控制信息: " + value);
                    }
                }).print();
        enev.execute();
    }
}
