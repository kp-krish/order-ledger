package com.example.orderledger.processing;

import java.util.UUID;

public record ProcessingResult(
        UUID eventId,
        ProcessingOutcome outcome,
        String detail
) {
}

