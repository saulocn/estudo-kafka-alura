package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;
    private final Class<T> type;
    private final Map<String, String> properties = new HashMap<>();

    KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type) {
        this(parse, groupId, type, new HashMap<>());
        consumer.subscribe(Collections.singletonList(topic));
    }

    KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
        this(parse, groupId, type, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
        this.properties.putAll(properties);
        this.type = type;
        this.parse = parse;
        this.consumer = new KafkaConsumer<String, T>(getProperties(groupId));
    }

    public void run() {
        while (true) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println(records.count() + "registros encontrados!");
                for (ConsumerRecord<String, T> record : records) {
                    try {
                        parse.consume(record);
                    } catch (ExecutionException e) {
                        // so far, just logging the exception for this message
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // so far, just logging the exception for this message
                        e.printStackTrace();
                    }
                }
            }
        }
    }
    private  Properties getProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        properties.putAll(this.properties);
        System.out.println(this.properties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
