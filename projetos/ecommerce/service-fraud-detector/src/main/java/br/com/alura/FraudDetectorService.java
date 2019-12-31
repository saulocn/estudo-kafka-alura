package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {
    public static void main(String[] args) {
        FraudDetectorService fraudService = new FraudDetectorService();
        try (KafkaService service = new KafkaService<Order>(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse,
                Order.class)){
            service.run();
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println(record.timestamp());
        System.out.println("-----------------------------------------");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }

        Order order = record.value();
        if(isFraud(order)){
            //pretendind fraud happpens when amount is higher or equal than 4500
            System.out.printf("Order is a Fraud!!!" + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order);
        } else {
            System.out.println("Approved! " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order);
        }
        System.out.println("Order Processed");
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500"))>=0;
    }
}
