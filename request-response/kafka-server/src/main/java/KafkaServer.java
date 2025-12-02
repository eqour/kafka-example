import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaServer {

    public static void main(String[] args) {
        String requestTopic = getRequiredEnv("APP_KAFKA_TOPIC_REQUEST");
        String responseTopic = getRequiredEnv("APP_KAFKA_TOPIC_RESPONSE");
        String kafkaServer = getRequiredEnv("APP_KAFKA_BOOTSTRAP_SERVERS");
        String consumerGroupId = getRequiredEnv("APP_KAFKA_CONSUMER_GROUP_ID");
        String key = "messages-key";

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());

        Gson gson = new Gson();

        try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
            consumer.subscribe(Collections.singletonList(requestTopic));

            System.out.println("Начало обработки сообщений");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    RequestDto request = gson.fromJson(record.value(), RequestDto.class);
                    ResponseDto response = handleRequest(request);
                    String responseJson = gson.toJson(response);

                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(responseTopic, key,
                            responseJson);
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Ошибка при отправке: " + exception.getMessage());
                        }
                    });
                }
            }
        }
    }

    private static String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Required environment variable is missing: " + name);
        }
        return value;
    }

    private static ResponseDto handleRequest(RequestDto request) {
        return new ResponseDto(request.getId(), request.getA() + request.getB());
    }
}
