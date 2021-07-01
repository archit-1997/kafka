package com.github.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        //creating a logger for the class
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create a producer <T,V>
        KafkaProducer <String,String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record <T,V> -> {topic , message on that topic}
        ProducerRecord<String,String> record = new ProducerRecord<>("first_topic","Hello from the other side");

        //send data with a callback
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executed every time when a record is sent successfully or an exception is thrown
                if (e == null) {
                    //record successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic : " + recordMetadata.topic() + "\n" +
                            "Partition : " + recordMetadata.partition() + "\n" +
                            "Offset : " + recordMetadata.offset() + "\n" +
                            "Timestamp : " + recordMetadata.timestamp());
                } else
                    logger.error("Error while producing ", e);
            }
        });

        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
