package com.adrien.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class PulsarConsumer {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("http://hadoop103:8077,hadoop104:8077,hadoop105:8077")
                .enableTlsHostnameVerification(false)
                .build();
        Consumer consumer = client.newConsumer()
                .topic("persistent://aikfk_tenant/aikfk_namespace/aikfk_topic_01")
                .subscriptionName("aikfk-subscription-sub") /**相当于 group**/
                .subscribe();
        Message msg = null;
        while (true) {
            // Wait for a message
             msg = consumer.receive();
            try {
                // Do something with the message
                System.out.println("Message received: " + new String(msg.getData()));
                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }
}
