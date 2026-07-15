import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Random;

public class SalesProducer {
    private static final String TOPIC = "sales-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096";
    private static final String[] PRODUCTS = {"Espresso Beans", "Manual Grinder", "Pour-over Dripper", "Sourdough Starter Kit"};

    public static void main(String[] args) throws Exception {
        String storeName = args.length > 0 ? args[0] : "Unknown_Store";
        System.out.println(" Starting producer for " + storeName + "...");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        try {
            while (true) {
                int id = 100000 + random.nextInt(900000);
                String item = PRODUCTS[random.nextInt(PRODUCTS.length)];
                double amount = 5.0 + (50.0 - 5.0) * random.nextDouble();
                amount = Math.round(amount * 100.0) / 100.0;

                Transaction tx = new Transaction(id, storeName, item, amount);
                String jsonValue = mapper.writeValueAsString(tx);

                // We make a final copy of the ID so the background callback thread can safely access it
                final int currentTxId = id; 

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, String.valueOf(currentTxId), jsonValue);
                System.out.println("Attempting to send Tx no." + id);
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Sent: " + jsonValue);
                    } else {
                        // 🟢 CUSTOM ERROR LOG WITH TRANSACTION ID
                        System.out.println("\n==================================================");
                        System.out.println(" ❌ PRODUCER ERROR DETECTED                       ");
                        System.out.println("==================================================");
                        System.out.format( "   [Tx ID]     #%d%n", currentTxId);
                        System.out.format( "   [Store]     %s%n", storeName);
                        System.out.format( "   [Status]    FAILED - Retrying in background...%n");
                        System.out.format( "   [Reason]    %s%n", exception.getMessage());
                        System.out.println("==================================================\n");
                    }
                });

                Thread.sleep(4000 + random.nextInt(2000));
            }
        } finally {
            producer.close();
        }
    }
}