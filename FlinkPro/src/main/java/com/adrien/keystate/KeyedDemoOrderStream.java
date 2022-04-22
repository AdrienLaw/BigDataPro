package com.adrien.keystate;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import static com.adrien.keystate.KeyedDemoOrderInfo1.string2OrderInfo1;
import static com.adrien.keystate.KeyedDemoOrderInfo2.string2OrderInfo2;

public class KeyedDemoOrderStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enev =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //配置状态后端
        enev.setStateBackend(new RocksDBStateBackend("hdfs://node01:8020/flink/checkDir",
                true));


        //每隔 1000 ms 开启一个检查点
        enev.enableCheckpointing(1000);
        enev.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        enev.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        enev.getCheckpointConfig().setCheckpointTimeout(6000);
        // 同一时间最多有一个检查点
        enev.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        /*
         * 表示一旦 Flink 处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
         * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
         * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        */
        enev.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);





        DataStreamSource<String> info1 =
                enev.addSource(new KeyedDemoFileSource(KeyedDemoConstants.ORDER_INFO1_PATH));
        DataStreamSource<String> info2 =
                enev.addSource(new KeyedDemoFileSource(KeyedDemoConstants.ORDER_INFO2_PATH));
        //操作
        KeyedStream<KeyedDemoOrderInfo1, Long> orderInfo1Stream = info1.map(line -> string2OrderInfo1(line))
                .keyBy(orderInfo1 -> orderInfo1.getOrderId());
        //12> KeyedDemoOrderInfo1(orderId=123, produceName=拖把, price=30.0)
        //12> KeyedDemoOrderInfo1(orderId=234, produceName=牙膏, price=20.0)
        //8> KeyedDemoOrderInfo1(orderId=333, produceName=杯子, price=112.2)
        //2> KeyedDemoOrderInfo1(orderId=345, produceName=被子, price=114.4)
        //11> KeyedDemoOrderInfo1(orderId=444, produceName=Mac电脑, price=30000.0)
        KeyedStream<KeyedDemoOrderInfo2, Long> orderInfo2Stream = info2.map(line -> string2OrderInfo2(line))
                .keyBy(orderInfo2 -> orderInfo2.getOrderId());
        //12> KeyedDemoOrderInfo2(orderId=234, orderDate=2020-11-11 11:11:13, address=云南)
        //12> KeyedDemoOrderInfo2(orderId=123, orderDate=2020-11-11 10:11:12, address=江苏)
        //2> KeyedDemoOrderInfo2(orderId=345, orderDate=2020-11-11 12:11:14, address=安徽)
        //8> KeyedDemoOrderInfo2(orderId=333, orderDate=2020-11-11 13:11:15, address=北京)
        //11> KeyedDemoOrderInfo2(orderId=444, orderDate=2020-11-11 14:11:16, address=深圳)

        orderInfo1Stream.connect(orderInfo2Stream)
                .flatMap(new KeyedDemoEnRichmentFunction())
                .print();
        enev.execute();


    }
}
