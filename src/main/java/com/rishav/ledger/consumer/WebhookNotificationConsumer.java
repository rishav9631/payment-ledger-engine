package com.rishav.ledger.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class WebhookNotificationConsumer {

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2),
            dltTopicSuffix = ".dlq"
    )
    @KafkaListener(topics = "ledger.transfers.completed", groupId = "webhook-consumer-group")
    public void onTransferCompleted(String payload) {
        log.info("Received transfer event for webhook notification: {}", payload);
        // In production, this would POST to a merchant webhook URL
        // with exponential backoff and circuit breaker pattern
    }
}
