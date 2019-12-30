package br.com.alura.ecommerce;

import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;

public class NewOrderMain {
    public static void main(String[] args)
            throws ExecutionException, InterruptedException {
        try(KafkaDispatcher dispatcher = new KafkaDispatcher()) {
            for (int i = 0; i < 10; i++) {
                String key = randomUUID().toString();
                String value = "1234,4321,31231.223";
                String email = "Thank you! We are processing your order!";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
