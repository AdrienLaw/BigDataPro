package com.adrien.wordcount;

import com.adrien.bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author luohaotian
 */
public class WordCountStreamSQL {
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
        Table table = tabEnv.fromDataStream(inputSource, "word,frequency");
        //执行SQL
        Table result = tabEnv.sqlQuery("select word,sum(frequency) as frequency from " + table + " group by word");
        tabEnv.toRetractStream(result,WordCount.class).print();
        sEnv.execute("WordCountStreamSQL");

        /*
         * [root@hadoop101 ~]# nc -lk 7777
         * I LOVE LK
         * I LOVE LONDON
         * I LOVE HK
         *
         * 9> (true,WordCount{word='LOVE', frequency=1})
         * 3> (true,WordCount{word='I', frequency=1})
         * 3> (true,WordCount{word='LK', frequency=1})
         *
         * 9> (false,WordCount{word='LOVE', frequency=1})
         * 3> (false,WordCount{word='I', frequency=1})
         * 3> (true,WordCount{word='I', frequency=2})
         * 9> (true,WordCount{word='LOVE', frequency=2})
         * 3> (true,WordCount{word='LONDON', frequency=1})
         *
         * 3> (false,WordCount{word='I', frequency=2})
         * 12> (true,WordCount{word='HK', frequency=1})
         *
         * 3> (true,WordCount{word='I', frequency=3})
         * 9> (false,WordCount{word='LOVE', frequency=2})
         * 9> (true,WordCount{word='LOVE', frequency=3})
         */
    }
}
