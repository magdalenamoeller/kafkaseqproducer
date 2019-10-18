package com.zanox;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SequenceProducer {
    public static void main(String args[]) {
        //System.out.println("polygenelubricants".hashCode());
        //System.out.println(Math.abs("polygenelubricants".hashCode()) < 0);

        //commit 1
        //commit 2

        String topic = "test"; //"args[0];
        String broker = "172.17.0.1:9092"; //"172.25.156.107:9092"; //"172.17.0.1:9092";//"localhost:9092";//"172.19.0.1:9092"; //args[1];

        new SequenceProducer(topic, broker);

        //commit 3 master
    }

    private SequenceProducer(String topic, String broker) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicInteger counter = new AtomicInteger(1);
        executor.submit((Runnable) () -> {
            KafkaProducer<String, String> producer = null;
            // Loop forever
            while (true) {
                boolean done = false;
                String current = String.valueOf(counter.getAndIncrement());
                do {
                    if (producer == null) {
                        producer = makeProducer(topic, broker);
                    }
                    try {
                        RecordMetadata m = producer.send(new ProducerRecord<>(
                                topic, current, current)
                        ).get();
                        System.err.println(String.format(
                                "%s - %d [%d] %s",
                                m.topic(), m.partition(), m.offset(), current
                        ));
                        done = true;
                    } catch (Exception e) {
                        producer.close();
                        producer = null;
                        System.err.println(e);
                    }
                } while (!done);
            }
        });
    }

    private static KafkaProducer<String, String> makeProducer(String topic, String broker) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("retries", Integer.MAX_VALUE);
        props.put("batch.size", 1);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }
}
