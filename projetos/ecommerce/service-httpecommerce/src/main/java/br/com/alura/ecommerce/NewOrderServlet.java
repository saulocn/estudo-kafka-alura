package br.com.alura.ecommerce;

import br.com.alura.KafkaDispatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;

public class NewOrderServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try(KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>()) {
            try(KafkaDispatcher emailDispatcher = new KafkaDispatcher<String>()) {
                try {
                    String email = req.getParameter("email");
                    String orderId = randomUUID().toString();
                    BigDecimal amount = new BigDecimal(req.getParameter("amount"));
                    Order order = new Order(email, orderId, amount);
                    String emailCode = "Thank you! We are processing your order!";

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
                    System.out.println("New order sent successfully!");
                    resp.setStatus(HttpServletResponse.SC_CREATED);
                    resp.getWriter().println("New order sent successfully!");
                } catch (ExecutionException e) {
                    throw new ServletException(e);
                } catch (InterruptedException e) {
                    throw new ServletException(e);
                }
            }
        }
    }
}
