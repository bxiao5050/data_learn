package io.test.bin;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerConfig.class.getSimpleName());

    public static void main(String[] args) {

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        for(int i =0; i<10;i++){
            String topic = "demo_java";
            String key = "id" + i;
            String value = "hello world"+1;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
            // send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null){
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "partition: " + metadata.partition() + "\n" +
                                "key: " + producerRecord.key() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp() + "\n"
                        );
                    }else {
                        log.error("Error with producing", exception);
                    }
                }
            });

        }

        //flush data
        producer.flush();

        //close producer
        producer.close();



    }
}

