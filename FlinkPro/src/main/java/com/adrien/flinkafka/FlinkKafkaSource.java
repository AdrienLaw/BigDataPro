package com.adrien.flinkafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置 检查点
//        enev.enableCheckpointing(100);
//        enev.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        enev.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        enev.getCheckpointConfig().setCheckpointTimeout(60000);
//        enev.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        enev.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //设置 状态后端
//        enev.setStateBackend(new RocksDBStateBackend("hdfs://hadoop101:9000/flink_kafka_sink/checkpoints",true));

        //kafka Source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","39.105.54.221:9092");
        properties.setProperty("zookeeper.connect","hadoop103:2181,hadoop104:2181,hadoop105:2181");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FlinkKafkaConsumer<String> kafkaConsumer011 = new FlinkKafkaConsumer<>("test",
                new SimpleStringSchema(),
                properties);
        //kafkaConsumer011.setCommitOffsetsOnCheckpoints(true);
        DataStream<String> kafkaSource = enev.addSource(kafkaConsumer011);
        kafkaSource.print();
        enev.execute();
    }
}
