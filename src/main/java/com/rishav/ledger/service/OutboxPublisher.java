package com.rishav.ledger.service;

import com.rishav.ledger.model.OutboxEvent;
import com.rishav.ledger.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;

@Service
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 500)
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository
                .findTop50ByStatusOrderByCreatedAtAsc("PENDING");

        for (OutboxEvent event : pendingEvents) {
            kafkaTemplate.send("ledger.transfers.completed",
                            event.getAggregateId().toString(),
                            event.getPayload())
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            event.setStatus("SENT");
                            event.setProcessedAt(OffsetDateTime.now());
                            outboxEventRepository.save(event);
                            log.info("Published outbox event {} to Kafka", event.getId());
                        } else {
                            log.error("Failed to publish outbox event {} to Kafka: {}",
                                    event.getId(), ex.getMessage());
                            // Event stays PENDING for retry on next poll
                        }
                    });
        }
    }
}
