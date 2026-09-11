package com.example.orderledger.processing;

import com.example.orderledger.event.OrderEvent;
import com.example.orderledger.event.OrderEventLine;
import com.example.orderledger.store.InventoryState;
import com.example.orderledger.store.LedgerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventProcessorTest {

    @Mock
    private LedgerRepository repository;

    private OrderEventProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new OrderEventProcessor(repository);
    }

    @Test
    void locksAndMutatesSkusInConsistentOrder() {
        var event = event(1, "order.placed", List.of(
                new OrderEventLine("SKU-B", 2),
                new OrderEventLine("SKU-A", 1)
        ));
        when(repository.findLastAppliedSequence(event.orderId(), event.eventId()))
                .thenReturn(OptionalLong.empty());
        when(repository.findOrderLines(event.orderId())).thenReturn(List.of());
        when(repository.lockInventory(List.of("SKU-A", "SKU-B"))).thenReturn(List.of(
                new InventoryState("SKU-A", 10, 0, false, 0),
                new InventoryState("SKU-B", 10, 0, false, 0)
        ));

        ProcessingResult result = processor.process(event);

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        verify(repository).lockInventory(List.of("SKU-A", "SKU-B"));
        verify(repository).reserveInventory(new OrderEventLine("SKU-A", 1));
        verify(repository).reserveInventory(new OrderEventLine("SKU-B", 2));
        verify(repository).markEvent(event.eventId(), "APPLIED");
    }

    @Test
    void rejectsAStaleSequenceBeforeTakingInventoryLocks() {
        var event = event(2, "order.cancelled", List.of(new OrderEventLine("SKU-A", 1)));
        when(repository.findLastAppliedSequence(event.orderId(), event.eventId()))
                .thenReturn(OptionalLong.of(3));

        ProcessingResult result = processor.process(event);

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.STALE);
        verify(repository, never()).lockInventory(List.of("SKU-A"));
        verify(repository).markEvent(event.eventId(), "STALE");
    }

    @Test
    void recordsDuplicateSkusAsAnInvalidEvent() {
        var event = event(1, "order.placed", List.of(
                new OrderEventLine("SKU-A", 1),
                new OrderEventLine("SKU-A", 2)
        ));
        when(repository.findLastAppliedSequence(event.orderId(), event.eventId()))
                .thenReturn(OptionalLong.empty());

        ProcessingResult result = processor.process(event);

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.INVALID);
        verify(repository).markEvent(event.eventId(), "INVALID");
    }

    private static OrderEvent event(long sequence, String type, List<OrderEventLine> lines) {
        return new OrderEvent(
                UUID.randomUUID(),
                1,
                type,
                "order-1",
                sequence,
                Instant.parse("2026-01-01T00:00:00Z"),
                lines
        );
    }
}

