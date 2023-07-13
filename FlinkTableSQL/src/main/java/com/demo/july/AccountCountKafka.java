package com.demo.july;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.util.Properties;

public class AccountCountKafka {
    public static void main(String[] args) throws Exception {
        //1 创建Flink的执行环境
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        // kafka 的消费属性
        Properties properties = new Properties();
        properties.setProperty("boostrap.servers","182.92.209.26:9092");
        properties.setProperty("group.id","flink-consumer");
        // 2 创建kafka消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("user-logs", new SimpleStringSchema(), properties);
        //添加 Kafka消费者数据源
        DataStreamSource<String> dataSource = enev.addSource(kafkaConsumer);

        //为每一条记录映射访问次数
        DataStream<Tuple2<String, Integer>> mappedStream = dataSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String log) throws Exception {
                String userId = parseUserId(log);
                return new Tuple2<>(userId, 1);
            }
        });
        //添加滑动窗口
        DataStream<Tuple2<String, Integer>> aggregatedStream = mappedStream
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1);
        aggregatedStream.print();

        enev.execute("User Access Count");
    }

    private static String parseUserId (String log) {
        String[] fields = log.split(" ");
        if (fields.length > 0) {
            return fields[0];
        }
        return " ";
    }
}

//bin/kafka-topics.sh --zookeeper hadoop103:2181 --create --topic user-logs --partitions 3 --replication-factor 3
