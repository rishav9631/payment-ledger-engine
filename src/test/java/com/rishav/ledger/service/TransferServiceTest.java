package com.rishav.ledger.service;

import com.rishav.ledger.dto.TransferRequest;
import com.rishav.ledger.dto.TransferResponse;
import com.rishav.ledger.exception.InsufficientBalanceException;
import com.rishav.ledger.model.Account;
import com.rishav.ledger.model.LedgerEntry;
import com.rishav.ledger.model.OutboxEvent;
import com.rishav.ledger.model.Transaction;
import com.rishav.ledger.repository.AccountRepository;
import com.rishav.ledger.repository.LedgerEntryRepository;
import com.rishav.ledger.repository.OutboxEventRepository;
import com.rishav.ledger.repository.TransactionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransferServiceTest {

    @Mock
    private AccountRepository accountRepository;
    @Mock
    private TransactionRepository transactionRepository;
    @Mock
    private LedgerEntryRepository ledgerEntryRepository;
    @Mock
    private OutboxEventRepository outboxEventRepository;
    @Mock
    private RedissonClient redissonClient;

    private MeterRegistry meterRegistry;
    private TransferService transferService;

    private UUID sourceId;
    private UUID targetId;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        transferService = new TransferService(
                accountRepository, transactionRepository, ledgerEntryRepository,
                outboxEventRepository, redissonClient, meterRegistry);

        sourceId = UUID.randomUUID();
        targetId = UUID.randomUUID();
    }

    private void setupLockMocks() throws InterruptedException {
        RLock lock1 = mock(RLock.class);
        RLock lock2 = mock(RLock.class);
        when(lock1.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(lock2.tryLock(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(true);
        when(lock1.isHeldByCurrentThread()).thenReturn(true);
        when(lock2.isHeldByCurrentThread()).thenReturn(true);
        when(redissonClient.getLock(anyString())).thenReturn(lock1, lock2);
    }

    @Test
    void shouldCreateCorrectLedgerEntries() throws InterruptedException {
        // Arrange
        setupLockMocks();
        String idempotencyKey = "test-key-1";

        when(transactionRepository.findByIdempotencyKey(idempotencyKey))
                .thenReturn(Optional.empty());

        Account source = Account.builder()
                .id(sourceId).ownerName("Alice").currency("INR")
                .balance(new BigDecimal("10000.0000")).build();
        Account target = Account.builder()
                .id(targetId).ownerName("Bob").currency("INR")
                .balance(new BigDecimal("5000.0000")).build();

        when(accountRepository.findByIdForUpdate(sourceId)).thenReturn(Optional.of(source));
        when(accountRepository.findByIdForUpdate(targetId)).thenReturn(Optional.of(target));

        UUID txId = UUID.randomUUID();
        when(transactionRepository.save(any(Transaction.class))).thenAnswer(inv -> {
            Transaction tx = inv.getArgument(0);
            tx.setId(txId);
            return tx;
        });
        when(ledgerEntryRepository.save(any(LedgerEntry.class))).thenAnswer(inv -> inv.getArgument(0));
        when(accountRepository.save(any(Account.class))).thenAnswer(inv -> inv.getArgument(0));
        when(outboxEventRepository.save(any(OutboxEvent.class))).thenAnswer(inv -> inv.getArgument(0));

        TransferRequest request = new TransferRequest(sourceId, targetId, new BigDecimal("500.00"), "INR");

        // Act
        TransferResponse response = transferService.executeTransfer(request, idempotencyKey);

        // Assert
        assertThat(response.status()).isEqualTo("COMPLETED");
        assertThat(response.transactionId()).isEqualTo(txId);

        // Verify double-entry: debit + credit entries
        ArgumentCaptor<LedgerEntry> entryCaptor = ArgumentCaptor.forClass(LedgerEntry.class);
        verify(ledgerEntryRepository, times(2)).save(entryCaptor.capture());

        var entries = entryCaptor.getAllValues();
        BigDecimal sum = entries.stream()
                .map(LedgerEntry::getAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        assertThat(sum).isEqualByComparingTo(BigDecimal.ZERO);

        assertThat(entries.get(0).getEntryType()).isEqualTo("DEBIT");
        assertThat(entries.get(0).getAmount()).isEqualByComparingTo(new BigDecimal("-500.00"));
        assertThat(entries.get(1).getEntryType()).isEqualTo("CREDIT");
        assertThat(entries.get(1).getAmount()).isEqualByComparingTo(new BigDecimal("500.00"));
    }

    @Test
    void shouldThrowInsufficientBalanceException() throws InterruptedException {
        // Arrange
        setupLockMocks();
        String idempotencyKey = "test-key-2";

        when(transactionRepository.findByIdempotencyKey(idempotencyKey))
                .thenReturn(Optional.empty());

        Account source = Account.builder()
                .id(sourceId).ownerName("Alice").currency("INR")
                .balance(new BigDecimal("100.0000")).build();
        Account target = Account.builder()
                .id(targetId).ownerName("Bob").currency("INR")
                .balance(new BigDecimal("5000.0000")).build();

        when(accountRepository.findByIdForUpdate(sourceId)).thenReturn(Optional.of(source));
        when(accountRepository.findByIdForUpdate(targetId)).thenReturn(Optional.of(target));

        TransferRequest request = new TransferRequest(sourceId, targetId, new BigDecimal("9999.00"), "INR");

        // Act & Assert
        assertThatThrownBy(() -> transferService.executeTransfer(request, idempotencyKey))
                .isInstanceOf(InsufficientBalanceException.class)
                .hasMessageContaining("Insufficient balance");
    }

    @Test
    void shouldReturnExistingTransactionForDuplicateIdempotencyKey() throws InterruptedException {
        // Arrange
        String idempotencyKey = "duplicate-key";
        UUID existingTxId = UUID.randomUUID();

        Transaction existingTx = Transaction.builder()
                .id(existingTxId)
                .idempotencyKey(idempotencyKey)
                .sourceAccountId(sourceId)
                .targetAccountId(targetId)
                .amount(new BigDecimal("500.00"))
                .status("COMPLETED")
                .createdAt(OffsetDateTime.now())
                .build();

        when(transactionRepository.findByIdempotencyKey(idempotencyKey))
                .thenReturn(Optional.of(existingTx));

        TransferRequest request = new TransferRequest(sourceId, targetId, new BigDecimal("500.00"), "INR");

        // Act
        TransferResponse response = transferService.executeTransfer(request, idempotencyKey);

        // Assert
        assertThat(response.transactionId()).isEqualTo(existingTxId);
        assertThat(response.status()).isEqualTo("COMPLETED");
        assertThat(response.message()).contains("idempotent");

        // Verify no new entities were created
        verify(accountRepository, never()).findByIdForUpdate(any());
        verify(ledgerEntryRepository, never()).save(any());
    }
}
