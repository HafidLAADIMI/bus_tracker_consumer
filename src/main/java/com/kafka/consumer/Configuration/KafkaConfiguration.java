package com.kafka.consumer.Configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

// this is will be the class for configuring the kafka   to consume the fake loction message coming from python script
@Configuration
public class KafkaConfiguration {

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("bus_location").build();
    }
}
