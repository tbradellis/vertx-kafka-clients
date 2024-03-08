import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class ProducerMain {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        // Kafka Producer Configuration
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, getConfig(new HashMap<String,String>()));

        // Sending a Kafka record
        KafkaProducerRecord<String, String> record;

        // Send the records
        for(int i = 0; i < 100; i++) {
            record = KafkaProducerRecord.create("your-topic", "key", "value" + i);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            producer.write(record);
            System.out.println("producer write " + record);

        }// Close the producer and Vert.x when done, using a callback to ensure it is done asynchronously
        producer.close(done -> {
            if (done.succeeded()) {
                System.out.println("Producer closed");
                vertx.close();
            } else {
                System.err.println("Error closing the producer");
            }
        });

    }

    private static Map<String, String> getConfig(Map<String, String> config){
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");
        return config;
    }
}
