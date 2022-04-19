package com.adrien.datastream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HerCustomSourceAndSink {

    public static class HerCustomSource implements SourceFunction<DataEntity> {
        // 标示位，控制数据产生
        private volatile boolean running = true;
        @Override
        public void run(SourceContext<DataEntity> ctx) throws Exception {
            //定义一个随机数
            Random random = new Random();
            //设置十个传感器温度
            HashMap<String, Double> dataTemp = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                dataTemp.put("id_" + (i + 1) , 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String senorId : dataTemp.keySet()) {
                    //在当前机组上随机波动
                    Double newTemp = dataTemp.get(senorId) + random.nextGaussian();
                    dataTemp.put(senorId,newTemp);
                    ctx.collect(new DataEntity(senorId,System.currentTimeMillis(),newTemp.longValue()));
                }
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }


}


