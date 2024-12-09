package com.kaab.producer;

import com.kaab.dto.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaAvroProducer {
    @Value("${topic.name}")
    private String topicName;
    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;


    public void send(Employee employee) {
        CompletableFuture<SendResult<String, Employee>> future = kafkaTemplate.send(topicName, UUID.randomUUID().toString(),employee);
        future.whenComplete((result, exception) -> {
           if (exception == null) {
               System.out.println("Send Employee=[ " + employee + "] with offset=[" + result.getRecordMetadata().offset() + "]");
           } else {
               System.out.println("Unable to send message=[ " + employee + "] due to " + exception.getMessage());
           }
        });
    }
}
