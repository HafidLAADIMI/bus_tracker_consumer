package com.kafka.consumer.Configuration;

import com.kafka.consumer.Models.BusTracker;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Sinks;

@Configuration
public class SinkConfig {

    @Bean
    public Sinks.Many<BusTracker> sinkLastStation() {
        return Sinks.many().multicast().onBackpressureBuffer();
    }

    @Bean
    public Sinks.Many<String> sinkSpeed() {
        return Sinks.many().multicast().onBackpressureBuffer();
    }
}
