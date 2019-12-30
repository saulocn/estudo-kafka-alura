package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;

public class NewOrderMain {
    public static void main(String[] args)
            throws ExecutionException, InterruptedException {
        try(KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>()) {
            try(KafkaDispatcher emailDispatcher = new KafkaDispatcher<String>()) {
                for (int i = 0; i < 10; i++) {
                    String userID = randomUUID().toString();
                    String orderId = randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
                    Order order = new Order(userID, orderId, amount);
                    String email = "Thank you! We are processing your order!";

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userID, order);
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userID, email);
                }
            }
        }
    }
}
