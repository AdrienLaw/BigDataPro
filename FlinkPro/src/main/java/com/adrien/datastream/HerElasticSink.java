package com.adrien.datastream;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;

public class HerElasticSink implements ElasticsearchSinkFunction<SensorReading> {
    @Override
    public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

        HashMap<Object, Object> dataSource = new HashMap<>();
        dataSource.put("id", sensorReading.getId());
        dataSource.put("ts", sensorReading.getTimestamp().toString());
        dataSource.put("temp", sensorReading.getTemperature().toString());
        IndexRequest indexRequest = Requests.indexRequest()
                .index("sensor")
                .type("doc")
                .source(dataSource);

        requestIndexer.add(indexRequest);
    }
}
