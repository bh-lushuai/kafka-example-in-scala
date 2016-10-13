package com.newapi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by lushuai on 16/10/13.
 */
public class ProducerDataTest {

    public static void main(String[] args) throws IOException {
        //String groupName = "consumer-indexCal-group";
        String groupName = "group01";
        //String topic ="index-calculate-topic";
        String topic ="test-topic01";
        // set up the producer
        KafkaProducer<String, String> producer;
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.143.90.22:6667,10.143.90.23:6667");
        props.put("group.id", groupName);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);

        try {
            for (int i = 1; i < 100; i++) {
                String msg=String.format("{" +
                        "    \"app_no\": \"%s\",\n" +
                        "    \"channel\":\"meiyifen\"\n" +"}", System.nanoTime());
                // send lots of messages
                producer.send(new ProducerRecord<String, String>(topic,msg));

                // every so often send to a different topic
                if (i % 10 == 0) {
                    producer.flush();
                    System.out.println("Send msg number " + i);
                }
            }
        } catch (Throwable throwable) {
            System.out.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
            System.out.printf("producer.close()");

        }

    }
}
