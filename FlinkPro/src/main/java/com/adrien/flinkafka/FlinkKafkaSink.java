package com.adrien.flinkafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

public class FlinkKafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev = StreamExecutionEnvironment.getExecutionEnvironment();
        enev.enableCheckpointing(5000);
        enev.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        enev.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        enev.getCheckpointConfig().setCheckpointTimeout(60000);
        enev.getCheckpointConfig().setMinPauseBetweenCheckpoints(1);

        enev.setStateBackend(new RocksDBStateBackend(
                "hdfs://hadoop101:9000/flink_kafka_sink/checkpoints",
                true));

        DataStreamSource<String> kafkaSourceStream = enev.socketTextStream("hadoop101", 9909);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hadooo101:9092,hadooo102:9092,hadooo103:9092,hadooo104:9092,hadooo105:9092");
        properties.setProperty("group.id","kafka_group1");
        //第一种解决方案，设置FlinkKafkaProducer里面的事务超时时间
        //设置事务超时时间
        properties.setProperty("transaction.timeout.ms",60000*15+"");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<String>("test",
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        DataStreamSink<String> stringDataStreamSink = kafkaSourceStream.addSink(kafkaSink);
        enev.execute();
    }
}
