package com.adrien.datastream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class HerSingleSource implements SourceFunction<Long> {

    private Long number = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
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
