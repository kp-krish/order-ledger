package com.example.orderledger.event;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderEventTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void acceptsTheVersionOneContract() {
        var event = event(1, "order.placed");

        assertThat(validator.validate(event)).isEmpty();
        assertThat(event.parsedType()).isEqualTo(OrderEventType.PLACED);
    }

    @Test
    void rejectsUnknownSchemaVersionsAndEventTypes() {
        assertThat(validator.validate(event(2, "order.returned")))
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactlyInAnyOrder("schemaVersion", "eventType");
    }

    @Test
    void rejectsUnknownWireValuesWhenParsed() {
        assertThatThrownBy(() -> OrderEventType.fromWireValue("order.returned"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("order.returned");
    }

    private static OrderEvent event(int schemaVersion, String eventType) {
        return new OrderEvent(
                UUID.randomUUID(),
                schemaVersion,
                eventType,
                "order-1",
                1,
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(new OrderEventLine("SKU-1", 2))
        );
    }
}

