package com.adrien.keystate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.List;


/**
 * Keyed state de ListState
 */
public class TestListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L),
                Tuple2.of(1L, 3L), Tuple2.of(1L, 6L), Tuple2.of(1L, 9L));
        dataStreamSource.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long,Double>>() {
                    //1. ListState 保存的是对应的一个 key 的出现的所有的元素
                    private ListState<Tuple2<Long, Long>> elementsByKey;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Tuple2<Long, Long>> listStateDescriptor = new ListStateDescriptor<Tuple2<Long, Long>>(
                                "average",
                                Types.TUPLE(Types.LONG,Types.LONG)
                        );
                        elementsByKey = getRuntimeContext().getListState(listStateDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
                        Iterable<Tuple2<Long, Long>> currentState = elementsByKey.get();
                        if (currentState == null) {
                            elementsByKey.addAll(Collections.emptyList());
                        }
                        //更新状态
                        elementsByKey.add(element);
                        //获取所有的元素
                        List<Tuple2<Long, Long>> allElements = Lists.newArrayList(elementsByKey.get());

                        if (allElements.size() >= 3) {
                            long count = 0;
                            long sum = 0;
                            for (Tuple2<Long, Long> ele : allElements) {
                                count++;
                                sum += ele.f1; }
                            double avg = (double) sum / count; out.collect(Tuple2.of(element.f0, avg));
                            // 清除状态
                            elementsByKey.clear();
                        }
                    }
                })
                .print();
        env.execute();
    }
}
