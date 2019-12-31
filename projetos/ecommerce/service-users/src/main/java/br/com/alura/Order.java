package br.com.alura;

import java.math.BigDecimal;

public class Order {
    private final String orderId;
    private final BigDecimal amount;
    private final String email;

    public Order(String email, String orderId, BigDecimal amount) {
        this.orderId = orderId;
        this.amount = amount;
        this.email = email;
    }

    public String getEmail() {
        return this.email;
    }

}
