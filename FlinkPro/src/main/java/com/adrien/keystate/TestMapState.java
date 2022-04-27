package com.adrien.keystate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/**
 * mapstate demo
 */
public class TestMapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(
                Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L),
                Tuple2.of(1L, 3L), Tuple2.of(1L, 6L), Tuple2.of(1L, 9L));
        dataStreamSource.keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long,Double>>() {
                    private MapState<String,Long> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>(
                                "average",
                                String.class, Long.class);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                    }

                    @Override
                    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
                        mapState.put(UUID.randomUUID().toString(), element.f1);
                        //取出状态里面的值
                        ArrayList<Long> allElements = Lists.newArrayList(mapState.values());
                        if (allElements.size() >= 3) {
                            long count = 0;
                            long sum = 0;
                            for (Long ele : allElements) {
                                count++;
                                sum += ele; }
                            double avg = (double) sum / count; out.collect(Tuple2.of(element.f0, avg));
                            // 清除状态
                            mapState.clear();
                        }
                    }
                }).print();

        env.execute();
    }
}
