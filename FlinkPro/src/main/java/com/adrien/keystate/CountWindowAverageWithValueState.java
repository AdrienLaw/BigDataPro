package com.adrien.keystate;

import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowAverageWithValueState
        extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Double>> {

    /**
     *  用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
     *  managed keyed state
     *  1. ValueState 保存的是对应的一个 key 的一个状态值
     */
    private ValueState<Tuple2<Long,Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> stateDescriptor = new ValueStateDescriptor<Tuple2<Long, Long>>(
                "average",
                Types.TUPLE(Types.LONG, Types.LONG)
        );
        countAndSum = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        Tuple2<Long, Long> currentState = countAndSum.value();
        //如果 当前状态 是null ，说明没被初始化。所以 要给附一个值
        if (currentState == null) {
            currentState = Tuple2.of(0L,0L);
        }
        // 更新状态值中的元素的个数
        currentState.f0 += 1;
        // 更新状态值中的总值 一个Key 下面的和
        currentState.f1 += value.f1;
        // 更新状态
        countAndSum.update(currentState);
        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        if (currentState.f0 >= 3) {
            double avg = (double) currentState.f1 / currentState.f0;
            // 输出 key 及其对应的平均值
            out.collect(Tuple2.of(value.f0,avg));
            //清空状态
            countAndSum.clear();
        }
    }
}
