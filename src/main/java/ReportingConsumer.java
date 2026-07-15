import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ReportingConsumer {
    private static final String TOPIC = "sales-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096";
    private static final String GROUP_ID = "reporting-revenue-group";

    public static void main(String[] args) throws Exception {
        System.out.println(" Reporting Consumer started. Waiting for transactions...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        ObjectMapper mapper = new ObjectMapper();
        double totalRevenue = 0.0;
        Map<String, Double> storeBreakdown = new HashMap<>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    Transaction tx = mapper.readValue(record.value(), Transaction.class);
                    
                    totalRevenue += tx.amount;
                    storeBreakdown.put(tx.source, storeBreakdown.getOrDefault(tx.source, 0.0) + tx.amount);

                    System.out.println("\n---LIVE CONSOLIDATED REPORT ---");
                    System.out.printf("Total Revenue: $%.2f%n", totalRevenue);
                    for (Map.Entry<String, Double> entry : storeBreakdown.entrySet()) {
                        System.out.printf("  [Store] %s: $%.2f%n", entry.getKey(), entry.getValue());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}