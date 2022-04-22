package com.adrien.keystate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author luohaotian
 */
public class KeyedDemoEnRichmentFunction extends RichCoFlatMapFunction<KeyedDemoOrderInfo1,KeyedDemoOrderInfo2, Tuple2<KeyedDemoOrderInfo1,KeyedDemoOrderInfo2>> {

    /**
     * 定义第两个 流 key对应的state
     */
    private ValueState<KeyedDemoOrderInfo1> orderInfo1State;
    private ValueState<KeyedDemoOrderInfo2> orderInfo2State;

    /**
     * 初始化两个状态
     * @param parameters yigecanshu
     * @throws Exception yigeyichang
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        orderInfo1State = getRuntimeContext().getState(new ValueStateDescriptor<KeyedDemoOrderInfo1>(
                "info1",
                KeyedDemoOrderInfo1.class
        ));
        orderInfo2State = getRuntimeContext().getState(new ValueStateDescriptor<KeyedDemoOrderInfo2>(
                "info2",
                KeyedDemoOrderInfo2.class
        ));
    }

    @Override
    public void flatMap1(KeyedDemoOrderInfo1 orderInfo1,
                         Collector<Tuple2<KeyedDemoOrderInfo1, KeyedDemoOrderInfo2>> out) throws Exception {
        KeyedDemoOrderInfo2 value2 = orderInfo2State.value();
        if (value2 != null) {
            orderInfo2State.clear();
            out.collect(Tuple2.of(orderInfo1,value2));
        } else {
            orderInfo1State.update(orderInfo1);
        }
    }

    @Override
    public void flatMap2(KeyedDemoOrderInfo2 orderInfo2,
                         Collector<Tuple2<KeyedDemoOrderInfo1, KeyedDemoOrderInfo2>> out) throws Exception {
        KeyedDemoOrderInfo1 value1 = orderInfo1State.value();
        if (value1 != null) {
            orderInfo1State.clear();
            out.collect(Tuple2.of(value1,orderInfo2));
        } else {
            orderInfo2State.update(orderInfo2);
        }
    }
}
