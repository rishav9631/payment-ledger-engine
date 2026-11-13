package com.rishav.ledger.controller;

import com.rishav.ledger.dto.TransferRequest;
import com.rishav.ledger.dto.TransferResponse;
import com.rishav.ledger.model.Account;
import com.rishav.ledger.model.LedgerEntry;
import com.rishav.ledger.model.Transaction;
import com.rishav.ledger.service.TransferService;
import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class TransferController {

    private final TransferService transferService;

    public TransferController(TransferService transferService) {
        this.transferService = transferService;
    }

    @PostMapping("/transfers")
    public ResponseEntity<TransferResponse> createTransfer(
            @Valid @RequestBody TransferRequest request,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {

        TransferResponse response = transferService.executeTransfer(request, idempotencyKey);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/transfers/{id}")
    public ResponseEntity<Transaction> getTransfer(@PathVariable UUID id) {
        Transaction transaction = transferService.findTransactionById(id)
                .orElseThrow(() -> new EntityNotFoundException("Transaction not found: " + id));
        return ResponseEntity.ok(transaction);
    }

    @GetMapping("/accounts/{id}/balance")
    public ResponseEntity<Map<String, Object>> getAccountBalance(@PathVariable UUID id) {
        Account account = transferService.findAccountById(id)
                .orElseThrow(() -> new EntityNotFoundException("Account not found: " + id));
        return ResponseEntity.ok(Map.of(
                "accountId", account.getId(),
                "balance", account.getBalance(),
                "currency", account.getCurrency()
        ));
    }

    @GetMapping("/accounts/{id}/ledger")
    public ResponseEntity<List<LedgerEntry>> getAccountLedger(@PathVariable UUID id) {
        List<LedgerEntry> entries = transferService.findLedgerEntriesByAccountId(id);
        return ResponseEntity.ok(entries);
    }
}
