import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ExampleProducer {

    public static void main(String[] args) {
        String topic = getRequiredEnv("APP_KAFKA_TOPIC");
        String kafkaServer = getRequiredEnv("APP_KAFKA_BOOTSTRAP_SERVERS");
        String key = "messages-key";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        System.out.println("Начало отправки сообщений");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 100; i++) {
                String message = "message #" + (i + 1);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Ошибка при отправке: " + exception.getMessage());
                    }
                    System.out.println("Отправлено сообщение " + message + " в партицию " + metadata.partition()
                            + ", offset: " + metadata.offset());
                });
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }

        System.out.println("Отправлены все сообщения");
    }

    private static String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Required environment variable is missing: " + name);
        }
        return value;
    }
}
