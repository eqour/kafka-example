import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ExampleConsumer {

    public static void main(String[] args) {
        String topic = getRequiredEnv("APP_KAFKA_TOPIC");
        String kafkaServer = getRequiredEnv("APP_KAFKA_BOOTSTRAP_SERVERS");
        String groupId = getRequiredEnv("APP_KAFKA_CONSUMER_GROUP_ID");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Начало обработки сообщений");

            int counter = 0;
            while (counter < 100) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Получено сообщение " + record.value() + ", партиция: " + record.partition()
                            + ", offset: " + record.offset());
                    Thread.sleep(500);
                    System.out.println("Сообщение обработано");
                    counter++;
                    System.out.println("Значение counter после обработки: " + counter);
                }
            }
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }

        System.out.println("Обработаны все сообщения");
    }

    private static String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Required environment variable is missing: " + name);
        }
        return value;
    }
}
