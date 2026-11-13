package com.rishav.ledger.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic transfersCompletedTopic() {
        return TopicBuilder.name("ledger.transfers.completed")
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic transfersDlqTopic() {
        return TopicBuilder.name("ledger.transfers.dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
