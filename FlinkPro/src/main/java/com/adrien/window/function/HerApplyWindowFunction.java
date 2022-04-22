package com.adrien.window.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HerApplyWindowFunction implements WindowFunction<Tuple2<String, Integer>, Double, Tuple, TimeWindow> {

    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Double> out) throws Exception {
        int totalNum = 0;
        int countNum = 0;
        for (Tuple2<String, Integer> element : input) {
            totalNum += 1;
            countNum += element.f1;
        }
        out.collect((double) (countNum/totalNum));

    }
}
