package com.rishav.ledger.dto;

import java.time.OffsetDateTime;
import java.util.UUID;

public record TransferResponse(
        UUID transactionId,
        String status,
        String message,
        OffsetDateTime processedAt
) {
}
