package com.adrien.datastream;

//https://blog.csdn.net/qq_41311979/article/details/114852904?spm=1001.2014.3001.5502
public class SourceFlinkDataStream {
//    public static void main(String[] args) {
//
//        //创建一个执行环境
//        StreamExecutionEnvironment streamExecutionEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        ExecutionEnvironment executionEnv = ExecutionEnvironment.getExecutionEnvironment();
//        //设置水位线
//        //environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//
//
//        //返回本地环境
//        LocalEnvironment localEnv = ExecutionEnvironment.createLocalEnvironment(2);
//        ExecutionEnvironment localEnvWebUI = ExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment.createLocalEnvironment(2);
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        //返回集群
//        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("flink-master", 8081, "/home/user/udfs.jar");
//        ExecutionEnvironment.createRemoteEnvironment("flink-master", 8081, "/home/user/udfs.jar");
//
//
//        LocalStreamEnvironment enev = StreamExecutionEnvironment.createLocalEnvironment();
//
//
//
//
//        /** source **/
//
//        DataStreamSource<String> sourceStream = enev.readTextFile("file://xxx");
//        DataStreamSource<String> socketTextStream = enev.socketTextStream("hadoop101", 9909);
//        // 从集合中读取数据
//        DataStreamSource<SensorReading> collectionSource = enev.fromCollection(Arrays.asList());
//        //读取单行数据
//        DataStreamSource<Long> addSingleSource = enev.addSource(new HerSingleSource());
//        // 读取多行数据
//        DataStreamSource<Long> addMyltipartSource = enev.addSource(new HerMultipartSource());
//
//
//        // 从kafka 中读取数据 kafka source
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop103:9092,hadoop104:9092,hadoop105:9092");
//        properties.setProperty("group.id", "flink-kafka");
//        properties.setProperty("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
//        DataStreamSource<String> kafkaSource = enev.addSource(new FlinkKafkaConsumer<String>(
//                "senor",
//                new SimpleStringSchema(),
//                properties
//        ));
//
//        //sink
//        //kafka
//        String brokerList = "hadoop103:9092,hadoop104:9092,hadoop105:9092";
//        String topic = "flink-kafka-sink";
//        DataStreamSink<String> kafkaSink = kafkaSource.addSink(new FlinkKafkaProducer<String>(brokerList, topic, new SimpleStringSchema()));
//
//        //redis
//        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
//                .setHost("hadoop102")
//                .setPort(7003)
//                .build();
//        //redis
//        collectionSource.addSink(new RedisSink<>(jedisPoolConfig, new HerRedisSInk()));
//
//
//        //ElasticSearch
//        List<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("hadoop103",9200));
//        httpHosts.add(new HttpHost("hadoop104",9200));
//        httpHosts.add(new HttpHost("hadoop105",9200));
//        collectionSource.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new HerElasticSink()).build());
//        //jdbc
//        collectionSource.addSink(new HerJdbcSink());
//
//
//    }

}
