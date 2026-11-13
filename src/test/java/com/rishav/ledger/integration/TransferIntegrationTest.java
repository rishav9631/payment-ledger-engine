package com.rishav.ledger.integration;

import com.rishav.ledger.dto.TransferRequest;
import com.rishav.ledger.dto.TransferResponse;
import com.rishav.ledger.model.Account;
import com.rishav.ledger.repository.AccountRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransferIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("ledger_test_db")
            .withUsername("test_user")
            .withPassword("test_password");

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.data.redis.host", () -> "localhost");
        registry.add("spring.data.redis.port", () -> "6379");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    private UUID sourceAccountId;
    private UUID targetAccountId;

    @BeforeEach
    void setUp() {
        accountRepository.deleteAll();

        Account source = Account.builder()
                .ownerName("Alice")
                .currency("INR")
                .balance(new BigDecimal("10000.0000"))
                .build();
        source = accountRepository.save(source);
        sourceAccountId = source.getId();

        Account target = Account.builder()
                .ownerName("Bob")
                .currency("INR")
                .balance(new BigDecimal("5000.0000"))
                .build();
        target = accountRepository.save(target);
        targetAccountId = target.getId();
    }

    @Test
    void shouldCompleteTransferSuccessfully() {
        TransferRequest request = new TransferRequest(
                sourceAccountId, targetAccountId, new BigDecimal("500.00"), "INR");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", UUID.randomUUID().toString());

        HttpEntity<TransferRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<TransferResponse> response = restTemplate.postForEntity(
                "/api/v1/transfers", entity, TransferResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().status()).isEqualTo("COMPLETED");
        assertThat(response.getBody().transactionId()).isNotNull();
    }

    @Test
    void shouldReturnSameResultForDuplicateIdempotencyKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        TransferRequest request = new TransferRequest(
                sourceAccountId, targetAccountId, new BigDecimal("200.00"), "INR");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", idempotencyKey);

        HttpEntity<TransferRequest> entity = new HttpEntity<>(request, headers);

        // First request
        ResponseEntity<TransferResponse> response1 = restTemplate.postForEntity(
                "/api/v1/transfers", entity, TransferResponse.class);

        // Second request with same idempotency key
        ResponseEntity<TransferResponse> response2 = restTemplate.postForEntity(
                "/api/v1/transfers", entity, TransferResponse.class);

        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response2.getStatusCode().is2xxSuccessful()).isTrue();

        // Both should reference the same transaction
        assertThat(response1.getBody()).isNotNull();
        assertThat(response2.getBody()).isNotNull();
        assertThat(response2.getBody().transactionId())
                .isEqualTo(response1.getBody().transactionId());
    }

    @Test
    void shouldReturnConflictForInsufficientBalance() {
        TransferRequest request = new TransferRequest(
                sourceAccountId, targetAccountId, new BigDecimal("99999.00"), "INR");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", UUID.randomUUID().toString());

        HttpEntity<TransferRequest> entity = new HttpEntity<>(request, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(
                "/api/v1/transfers", entity, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }
}
