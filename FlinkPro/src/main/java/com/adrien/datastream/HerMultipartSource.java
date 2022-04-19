package com.adrien.datastream;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class HerMultipartSource implements ParallelSourceFunction<Long> {

    private Long number = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (true) {
            number += 1;
            ctx.collect(number);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
