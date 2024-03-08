import io.vertx.core.Vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerVerticle extends AbstractVerticle {

    @Override
    public void start() {
        // Kafka consumer configuration
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("group.id", "your-consumer-group");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Create Kafka consumer
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

        // Subscribe to a Kafka topic
        consumer.subscribe("your-topic", ar -> {
            if (ar.succeeded()) {
                System.out.println("Subscribed to topic: your-topic");
            } else {
                System.err.println("Failed to subscribe to topic: " + ar.cause().getMessage());
            }
        });

        // Handle incoming Kafka messages
        consumer.handler(record -> {
            KafkaConsumerRecord<String, String> kafkaRecord = (KafkaConsumerRecord<String, String>) record;
            System.out.println("Received message: " + kafkaRecord.value());
        });
    }

    public static void main(String[] args) {
        // Deploy the Verticle
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new KafkaConsumerVerticle());

        // Register a shutdown hook to close the Vert.x instance
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Vert.x...");
            vertx.close();
        }));
    }
}
