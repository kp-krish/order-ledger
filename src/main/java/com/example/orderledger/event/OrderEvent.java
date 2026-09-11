package com.example.orderledger.event;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderEvent(
        @NotNull UUID eventId,
        @Min(1) @Max(1) int schemaVersion,
        @NotBlank @Pattern(regexp = "order\\.(placed|cancelled|shipped)") String eventType,
        @NotBlank String orderId,
        @Positive long sequence,
        @NotNull Instant occurredAt,
        @NotEmpty List<@Valid OrderEventLine> lines
) {
    public OrderEvent {
        lines = lines == null ? null : List.copyOf(lines);
    }

    public OrderEventType parsedType() {
        return OrderEventType.fromWireValue(eventType);
    }
}
