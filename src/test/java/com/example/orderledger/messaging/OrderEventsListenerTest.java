package com.example.orderledger.messaging;

import com.example.orderledger.event.OrderEvent;
import com.example.orderledger.event.OrderEventLine;
import com.example.orderledger.processing.OrderEventProcessor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventsListenerTest {

    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private Validator validator;
    @Mock
    private OrderEventProcessor processor;

    private SimpleMeterRegistry meterRegistry;
    private OrderEventsListener listener;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        listener = new OrderEventsListener(objectMapper, validator, processor, meterRegistry);
    }

    @Test
    void acknowledgesADatabaseDetectedDuplicate() throws Exception {
        OrderEvent event = event();
        when(objectMapper.readValue(anyString(), eq(OrderEvent.class))).thenReturn(event);
        when(validator.validate(event)).thenReturn(Set.of());
        when(processor.process(event)).thenThrow(new DuplicateKeyException("duplicate event_id"));

        listener.onEvent(record(event.orderId()));

        verify(processor).process(event);
        assertThat(meterRegistry.get("order.ledger.events")
                .tag("outcome", "duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    void rejectsARecordWhoseKeyDoesNotMatchItsOrder() throws Exception {
        OrderEvent event = event();
        when(objectMapper.readValue(anyString(), eq(OrderEvent.class))).thenReturn(event);
        when(validator.validate(event)).thenReturn(Set.of());

        assertThatThrownBy(() -> listener.onEvent(record("another-order")))
                .isInstanceOf(NonRetryableEventException.class)
                .hasMessageContaining("Kafka key");
        assertThat(meterRegistry.get("order.ledger.events")
                .tag("outcome", "rejected").counter().count()).isEqualTo(1.0);
    }

    private static ConsumerRecord<String, String> record(String key) {
        return new ConsumerRecord<>("orders.events", 0, 0, key, "{}");
    }

    private static OrderEvent event() {
        return new OrderEvent(
                UUID.randomUUID(),
                1,
                "order.placed",
                "order-1",
                1,
                Instant.parse("2026-01-01T00:00:00Z"),
                List.of(new OrderEventLine("SKU-1", 1))
        );
    }
}

