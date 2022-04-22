package com.adrien.keystate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;


public class CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private ListState<Tuple2<Long,Long>> elementsByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Tuple2<Long, Long>> listStateDescriptor =
                new ListStateDescriptor<Tuple2<Long, Long>>(
                "average",
                    // 状态的名字
                    Types.TUPLE(Types.LONG, Types.LONG));
        elementsByKey = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        // 拿到当前的 key 的状态值
        Iterable<Tuple2<Long, Long>> currentState = elementsByKey.get();
        // 如果状态值还没有初始化，则初始化
        if (currentState == null) {
            elementsByKey.addAll(Collections.emptyList());
        }
        // 更新状态
        elementsByKey.add(value);
        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        List<Tuple2<Long, Long>> allElements = Lists.newArrayList(elementsByKey.get());
        if (allElements.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Tuple2<Long, Long> allElement : allElements) {
                count ++;
                sum += allElement.f1;
            }
            double avage = (double) sum / count;
            out.collect(Tuple2.of(value.f0,avage));
            //清除状态
            elementsByKey.clear();
        }
    }
}
