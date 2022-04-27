package com.adrien.checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class TestCheckpoint {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                // 间隔)
                Time.of(10, TimeUnit.SECONDS))); //5

//        env.setStateBackend(new MemoryStateBackend());
//        env.setStateBackend(new FsStateBackend("hdfs:xxx"));
//        env.setStateBackend(new RocksDBStateBackend("hdfs:xx"));

        //设置checkpoint //建议不要太短
        //每隔多久执行一下checkpoint
        //个人建议在生产里面，5分钟左右checkpoint一次就可以了。
        env.enableCheckpointing(5000);
        //设置StateBackend类型
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9000/flink/checkpoint"));
        //设置checkpoint支持的语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //两次checkpoint的时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        //最多1个checkpoints同时进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //checkpoint超时的时间
        //默认值是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //cancel程序的时候保存checkpoint
        //checkpoint数据
        //比如我们停止了Flink的任务
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> dataStream = env.socketTextStream("hadoop101", 9909);
        SingleOutputStreamOperator<String> result = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                return "nx_" + line;
            }
        }).uid("11111");
        result.print().uid("print-operator");

        env.execute("testCheckpint5Seconds ");
    }
}
