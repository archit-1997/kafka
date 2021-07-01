package com.github.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        //key and value both are going to be string
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create a producer <K,V>
        KafkaProducer <String,String> producer = new KafkaProducer<>(properties);

        //create a producer record <K,V> -> {topic , message on that topic}
        ProducerRecord<String,String> record = new ProducerRecord<>("first_topic","hello world");

        //send data
        producer.send(record);

        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
