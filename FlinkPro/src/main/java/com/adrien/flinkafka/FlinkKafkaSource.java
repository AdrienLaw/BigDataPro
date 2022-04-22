package com.adrien.flinkafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FlinkKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置 检查点
        enev.enableCheckpointing(100);
        enev.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        enev.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        enev.getCheckpointConfig().setCheckpointTimeout(60000);
        enev.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        enev.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置 状态后端
        enev.setStateBackend(new RocksDBStateBackend("hdfs://hadoop101:8020/flink_kafka_sink/checkpoints",
                true));

        //kafka Source
        Properties properties = new Properties();
        properties.setProperty("group_id","conn1");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties);
        kafkaConsumer011.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> kafkaSource = enev.addSource(kafkaConsumer011);
        kafkaSource.print();
        enev.execute();
    }
}
