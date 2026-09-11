package com.example.orderledger.event;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

public record OrderEventLine(
        @NotBlank String sku,
        @Positive int qty
) {
}

