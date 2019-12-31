package br.com.alura;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.*;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url  = "jdbc:sqlite:target/users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users(" +
                    "uuid varchar(200) primary key," +
                    "email varchhar(200))");
        } catch (SQLException e){
            // Be careful, the sql could be really wront
        }
    }

    public static void main(String[] args) throws SQLException {
        CreateUserService createUserService = new CreateUserService();
        try (KafkaService service = new KafkaService<Order>(
                CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class)){
            service.run();
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("-----------------------------------------");
        System.out.println("Processing new order, checking for new User");
        System.out.println(record.value());
        Order order = record.value();
        if(isNewUSer(order.getEmail())){
            insertNewUser(UUID.randomUUID().toString(), order.getEmail());
        }
    }

    private void insertNewUser(String uuid, String email) throws SQLException {
        PreparedStatement insert = connection.prepareStatement("INSERT INTO Users(uuid, email) VALUES (?,?)");
        insert.setString(1, uuid);
        insert.setString(2, email);
        insert.execute();
        System.out.println("Usuário uuid e "+email+ " adicionado!");

    }

    private boolean isNewUSer(String email) throws SQLException {
        PreparedStatement exists = connection.prepareStatement("SELECT uuid from Users where email = ? limit 1");
        exists.setString(1, email);
        ResultSet rs = exists.executeQuery();
        return !rs.next();
    }

}
