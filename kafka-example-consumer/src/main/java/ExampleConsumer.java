import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ExampleConsumer {

    public static void main(String[] args) {
        String topic = "messages";
        String kafkaServer = "kafka:9092"; // localhost without docker
        String groupId = "consumer-group";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Начало отправки сообщений");

            int counter = 0;
            while (counter < 100) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Получено сообщение " + record.value() + ", партиция: " + record.partition()
                            + ", offset: " + record.offset());
                    Thread.sleep(500);
                    System.out.println("Сообщение обработано");
                    counter++;
                }
            }
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        }

        System.out.println("Обработаны все сообщения");
    }
}
