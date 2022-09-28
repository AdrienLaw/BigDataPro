package com.adrien.pulsar;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

public class PulsarProduce {
    public static void main(String[] args) throws Exception{
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://hadoop103:6650," +
                        "hadoop104:6650," +
                        "hadoop105:6650")
                .build();
        Producer<byte[]> producer = client.newProducer()
                .topic("persistent://aikfk_tenant/aikfk_namespace/aikfk_topic_01")
                .create();

// You can then send messages to the broker and topic you specified:
        producer.send("达拉崩吧".getBytes());
        producer.close();
    }
}
