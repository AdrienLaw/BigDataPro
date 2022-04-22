package com.adrien.transfor;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTransformMapFilter {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //创建数据源
        DataStreamSource<String> integerDataStreamSource = enev.fromElements("东邪", "西毒", "南帝", "北丐", "中神通");
        /* map */
        SingleOutputStreamOperator<String> transMap = integerDataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return new StringBuffer().append(value).append(" 997").toString();
            }
        });
        /**
         * 4> 北丐 997
         * 5> 中神通 997
         * 3> 南帝 997
         * 1> 东邪 997
         * 2> 西毒 997
         */
        /* filter */
        SingleOutputStreamOperator<String> transFilter = transMap.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                int length = value.length();
                return value.length() > 6;
            }
        });
        transFilter.print();
        //11> 中神通 997
        enev.execute();
    }
}
