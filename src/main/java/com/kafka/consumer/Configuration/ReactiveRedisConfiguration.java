package com.kafka.consumer.Configuration;

import com.kafka.consumer.Models.Station;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class ReactiveRedisConfiguration {
    @Bean
    ReactiveRedisOperations<String, Station> redisOperations(ReactiveRedisConnectionFactory factory) {
        Jackson2JsonRedisSerializer<Station> serializer = new Jackson2JsonRedisSerializer<>(Station.class);

        RedisSerializationContext.RedisSerializationContextBuilder<String, Station> builder =
                RedisSerializationContext.newSerializationContext(new StringRedisSerializer());

        RedisSerializationContext<String, Station> context = builder.value(serializer).build();

        return new ReactiveRedisTemplate<>(factory, context);
    }
}
