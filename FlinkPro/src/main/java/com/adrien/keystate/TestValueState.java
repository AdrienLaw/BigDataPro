package com.adrien.keystate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L), Tuple2.of(2L, 4L),
                Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));

        dataStreamSource
                .keyBy(0)
                //flatMap 输入 一个 Long,Long  输出的是 Long,Double
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long,Double>>() {
                    //1. ValueState 保存的是对应的一个 key 的一个状态值
                    //每一个 key 都有这样一个状态
                    private ValueState<Tuple2<Long,Long>> countAndSumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //注册一个状态
                        ValueStateDescriptor<Tuple2<Long, Long>> stateDescriptor = new ValueStateDescriptor<Tuple2<Long, Long>>(
                                "average",
                                Types.TUPLE(Types.LONG,Types.LONG)
                        );
                        countAndSumState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
                        // 拿到当前的 key 的状态值
                        Tuple2<Long, Long> currentState = countAndSumState.value();
                        // 如果状态值还没有初始化，则初始化
                        if (currentState == null) {
                            currentState = Tuple2.of(0L, 0L);
                        }
                        // 更新状态值中的元素的个数
                        currentState.f0 += 1;
                        // 更新状态值中的总值
                        currentState.f1 += element.f1;
                        // 更新状态
                        countAndSumState.update(currentState);
                        //判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
                        if (currentState.f0 >= 3) {
                            double avg = (double)currentState.f1 / currentState.f0;
                            // 输出 key 及其对应的平均值
                            out.collect(Tuple2.of(element.f0, avg)); // 清空状态值
                            countAndSumState.clear();
                        }
                    }
                })
                .print();
        env.execute("TestStatefulApi");
    }
}
