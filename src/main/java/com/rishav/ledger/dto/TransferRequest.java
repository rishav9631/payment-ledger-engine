package com.rishav.ledger.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.UUID;

public record TransferRequest(
        @NotNull UUID sourceAccountId,
        @NotNull UUID targetAccountId,
        @NotNull @Positive BigDecimal amount,
        String currency
) {
    public TransferRequest {
        if (currency == null || currency.isBlank()) {
            currency = "INR";
        }
    }
}
