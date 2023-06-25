package com.adrien.wordcount;

import com.adrien.bean.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author luohaotian
 */
public class WordCountBatchSQL {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromElements("I love Paris", "I love LA", "I love London");

        BatchTableEnvironment tabEnv = BatchTableEnvironment.create(env);
        FlatMapOperator<String, WordCount> input = dataSource.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] words = value.split(" ");
                for (String w : words) {
                    //   k2,v2
                    out.collect(new WordCount(w, 1));
                }
            }
        });

        Table table = tabEnv.fromDataSet(input);
        tabEnv.createTemporaryView("wordCount",table);
        //执行SQL
        Table table1 = tabEnv.sqlQuery("select word,sum(frequency) as frequency from wordCount group by word");

        DataSet<WordCount> result = tabEnv.toDataSet(table1, WordCount.class);

        result.print();
//        WordCount{word='I', frequency=3}
//        WordCount{word='love', frequency=3}
//        WordCount{word='LA', frequency=1}
//        WordCount{word='Paris', frequency=1}
//        WordCount{word='London', frequency=1}

    }

}
