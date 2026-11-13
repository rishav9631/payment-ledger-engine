package com.rishav.ledger.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rishav.ledger.dto.TransferRequest;
import com.rishav.ledger.dto.TransferResponse;
import com.rishav.ledger.exception.InsufficientBalanceException;
import com.rishav.ledger.model.*;
import com.rishav.ledger.repository.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.persistence.EntityNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TransferService {

    private final AccountRepository accountRepository;
    private final TransactionRepository transactionRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final RedissonClient redissonClient;
    private final Counter transferCounter;
    private final Timer transferTimer;
    private final ObjectMapper objectMapper;

    public TransferService(AccountRepository accountRepository,
                           TransactionRepository transactionRepository,
                           LedgerEntryRepository ledgerEntryRepository,
                           OutboxEventRepository outboxEventRepository,
                           RedissonClient redissonClient,
                           MeterRegistry meterRegistry) {
        this.accountRepository = accountRepository;
        this.transactionRepository = transactionRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.redissonClient = redissonClient;
        this.transferCounter = meterRegistry.counter("transfers.completed.total");
        this.transferTimer = meterRegistry.timer("transfers.duration");
        this.objectMapper = new ObjectMapper();
    }

    @Transactional
    public TransferResponse executeTransfer(TransferRequest request, String idempotencyKey) {
        // 1. Check idempotency — return existing result if already processed
        Optional<Transaction> existing = transactionRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            Transaction tx = existing.get();
            return new TransferResponse(tx.getId(), tx.getStatus(),
                    "Transfer already processed (idempotent)", tx.getCreatedAt());
        }

        // 2. Acquire sorted locks to prevent deadlocks
        String id1 = request.sourceAccountId().toString();
        String id2 = request.targetAccountId().toString();
        String firstLock = id1.compareTo(id2) < 0 ? id1 : id2;
        String secondLock = id1.compareTo(id2) < 0 ? id2 : id1;

        RLock lock1 = redissonClient.getLock("account-lock:" + firstLock);
        RLock lock2 = redissonClient.getLock("account-lock:" + secondLock);

        long start = System.nanoTime();
        try {
            boolean acquired1 = lock1.tryLock(5, 30, TimeUnit.SECONDS);
            boolean acquired2 = lock2.tryLock(5, 30, TimeUnit.SECONDS);

            if (!acquired1 || !acquired2) {
                throw new RuntimeException("Could not acquire locks for transfer. Please retry.");
            }

            // 3. Fetch accounts with pessimistic lock
            Account source = accountRepository.findByIdForUpdate(request.sourceAccountId())
                    .orElseThrow(() -> new EntityNotFoundException(
                            "Source account not found: " + request.sourceAccountId()));
            Account target = accountRepository.findByIdForUpdate(request.targetAccountId())
                    .orElseThrow(() -> new EntityNotFoundException(
                            "Target account not found: " + request.targetAccountId()));

            // Validate sufficient balance
            if (source.getBalance().compareTo(request.amount()) < 0) {
                throw new InsufficientBalanceException(
                        "Insufficient balance in account " + source.getId() +
                                ". Available: " + source.getBalance() + ", Required: " + request.amount());
            }

            // 4. Create Transaction
            Transaction transaction = Transaction.builder()
                    .idempotencyKey(idempotencyKey)
                    .sourceAccountId(request.sourceAccountId())
                    .targetAccountId(request.targetAccountId())
                    .amount(request.amount())
                    .status("COMPLETED")
                    .build();
            transaction = transactionRepository.save(transaction);

            // 5. Create double-entry ledger entries
            LedgerEntry debit = LedgerEntry.builder()
                    .transactionId(transaction.getId())
                    .accountId(request.sourceAccountId())
                    .entryType("DEBIT")
                    .amount(request.amount().negate())
                    .build();

            LedgerEntry credit = LedgerEntry.builder()
                    .transactionId(transaction.getId())
                    .accountId(request.targetAccountId())
                    .entryType("CREDIT")
                    .amount(request.amount())
                    .build();

            ledgerEntryRepository.save(debit);
            ledgerEntryRepository.save(credit);

            // 6. Update account balances
            source.setBalance(source.getBalance().subtract(request.amount()));
            target.setBalance(target.getBalance().add(request.amount()));
            accountRepository.save(source);
            accountRepository.save(target);

            // 7. Create outbox event
            String payload;
            try {
                payload = objectMapper.writeValueAsString(Map.of(
                        "transactionId", transaction.getId().toString(),
                        "sourceAccountId", request.sourceAccountId().toString(),
                        "targetAccountId", request.targetAccountId().toString(),
                        "amount", request.amount().toPlainString()
                ));
            } catch (Exception e) {
                payload = "{}";
            }

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("TRANSFER")
                    .aggregateId(transaction.getId())
                    .eventType("TRANSFER_COMPLETED")
                    .payload(payload)
                    .build();
            outboxEventRepository.save(outboxEvent);

            // 9. Record metrics
            transferCounter.increment();
            transferTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);

            log.info("Transfer completed: {} -> {}, amount={}, txId={}",
                    request.sourceAccountId(), request.targetAccountId(),
                    request.amount(), transaction.getId());

            // 11. Return response
            return new TransferResponse(
                    transaction.getId(),
                    "COMPLETED",
                    "Transfer processed successfully",
                    transaction.getCreatedAt()
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Lock acquisition interrupted", e);
        } finally {
            // 10. Release locks
            if (lock1.isHeldByCurrentThread()) {
                lock1.unlock();
            }
            if (lock2.isHeldByCurrentThread()) {
                lock2.unlock();
            }
        }
    }

    public Optional<Transaction> findTransactionById(UUID id) {
        return transactionRepository.findById(id);
    }

    public Optional<Account> findAccountById(UUID id) {
        return accountRepository.findById(id);
    }

    public java.util.List<LedgerEntry> findLedgerEntriesByAccountId(UUID accountId) {
        return ledgerEntryRepository.findByAccountId(accountId);
    }
}
