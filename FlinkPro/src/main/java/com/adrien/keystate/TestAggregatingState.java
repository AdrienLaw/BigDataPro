package com.adrien.keystate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TestAggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));
        dataStreamSource.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long,String>>() {
                    private AggregatingState<Long, String> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<Long, String, String> aggregatingStateDescriptor =
                                new AggregatingStateDescriptor<>(
                                    "totalStr",
                                    new AggregateFunction<Long, String, String>() {
                                        @Override
                                        public String createAccumulator() {
                                            return "Contains:";
                                        }

                                        @Override
                                        public String add(Long value, String accumulator) {
                                            if ("Contains:".equals(accumulator)) {
                                                return accumulator + value;
                                            }
                                            return accumulator + " and " + value;
                                        }

                                        @Override
                                        public String getResult(String accumulator) {
                                            return accumulator;
                                        }

                                        @Override
                                        public String merge(String a, String b) {
                                            return a + " and " + b;
                                        }
                                }, String.class
                        );
                        aggregatingState= getRuntimeContext().getAggregatingState(aggregatingStateDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, String>> out) throws Exception {
                        aggregatingState.add(element.f1);
                        out.collect(Tuple2.of(element.f0, aggregatingState.get()));
                    }
                }).print();

        env.execute();
    }
}
