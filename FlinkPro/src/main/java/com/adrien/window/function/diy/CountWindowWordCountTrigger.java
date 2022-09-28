package com.adrien.window.function.diy;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CountWindowWordCountTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop101", 9909);
        DataStreamSink<Tuple2<String, Integer>> trigger = dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String word : splits) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }).keyBy(0)
                .window(GlobalWindows.create())
                .trigger(new Trigger<Tuple2<String, Integer>, GlobalWindow>() {
                    // 表示指定的元素的最大的数量
                    private long maxCount = 3;
                    private ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<Long>(
                            "reduceState",
                            new ReduceFunction<Long>() {
                                @Override
                                public Long reduce(Long value1, Long value2) throws Exception {
                                    return value1 + value2;
                                }
                            },Long.class);

                    /**
                     * 当一个元素进入到一个 window 中的时候就会调用这个方法
                     * @param element 元素
                     * @param timestamp 进来的时间
                     * @param window 元素所属的窗口
                     * @param ctx 上下文
                     * @return TriggerResult
                     *
                     * 1. TriggerResult.CONTINUE :表示对 window 不做任何处理
                     * 2. TriggerResult.FIRE :表示触发 window 的计算
                     * 3. TriggerResult.PURGE :表示清除 window 中的所有数据
                     * 4. TriggerResult.FIRE_AND_PURGE :表示先触发 window 计算，然后删除window 中的数据
                     * @throws Exception
                     */
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
                        // 拿到当前 key 对应的 count 状态值
                        ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
                        // count 累加 1
                        count.add(1L);
                        if (count.get() == maxCount) {
                            count.clear();
                            // 触发 window 计算，删除数据
                            return TriggerResult.FIRE_AND_PURGE;
                        }
                        return TriggerResult.CONTINUE;
                    }

                    // 写基于 Processing Time 的定时器任务逻辑
                    @Override
                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    // 写基于 Event Time 的定时器任务逻辑
                    @Override
                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }
                    // 清除状态值
                    @Override
                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
                        ctx.getPartitionedState(stateDescriptor).clear();
                    }
                }).sum(1).print().setParallelism(1);
        env.execute("Streaming WordCount");
    }
}
