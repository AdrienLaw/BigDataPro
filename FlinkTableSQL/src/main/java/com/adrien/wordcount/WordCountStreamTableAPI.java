package com.adrien.wordcount;

import com.adrien.bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

public class WordCountStreamTableAPI {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表环境
        DataStreamSource<String> streamSource = sEnv.socketTextStream("hadoop101", 7777);
        //创建输入数据
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(sEnv);

        SingleOutputStreamOperator<WordCount> inputSource = streamSource.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] words = value.split(" ");
                for (String w : words) {
                    //   k2,v2
                    out.collect(new WordCount(w, 1));
                }
            }
        });
        Table table = tabEnv.fromDataStream(inputSource,"word,frequency");
        Table result = table.groupBy($("word"))
                .select($("word"),$("frequency").sum().as("frequency"))
                .as("word","frequency");
        //输出
        tabEnv.toRetractStream(result,WordCount.class).print();
        sEnv.execute("WordCountStreamTableAPI");
    }
}
