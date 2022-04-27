package com.adrien.keystate;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestReduceState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long,Long>>() {
                    private ReducingState<Long> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<Long> reducingStateDescriptor = new ReducingStateDescriptor<>(
                                "average",
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long value1, Long value2) throws Exception {
                                        return value1 + value2;
                                    }
                                }, Long.class);
                        reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> out) throws Exception {
                        reducingState.add(element.f1);
                        out.collect(Tuple2.of(element.f0,reducingState.get()));
                    }
                }).print();
        env.execute();
    }
}
