package com.example.orderledger.processing;

import com.example.orderledger.event.OrderEvent;
import com.example.orderledger.event.OrderEventLine;
import com.example.orderledger.event.OrderEventType;
import com.example.orderledger.store.InventoryState;
import com.example.orderledger.store.LedgerRepository;
import com.example.orderledger.store.OrderLineState;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderEventProcessor {

    private final LedgerRepository repository;

    public OrderEventProcessor(LedgerRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public ProcessingResult process(OrderEvent event) {
        repository.lockOrder(event.orderId());
        repository.insertEvent(event);

        var lastSequence = repository.findLastAppliedSequence(event.orderId(), event.eventId());
        if (lastSequence.isPresent() && event.sequence() <= lastSequence.getAsLong()) {
            return finish(event, ProcessingOutcome.STALE,
                    "sequence %d is not newer than %d".formatted(event.sequence(), lastSequence.getAsLong()));
        }

        var linesBySku = uniqueLines(event.lines());
        if (linesBySku == null) {
            return finish(event, ProcessingOutcome.INVALID, "an event cannot contain the same SKU twice");
        }

        return switch (event.parsedType()) {
            case PLACED -> place(event, linesBySku);
            case CANCELLED -> changeStatus(event, linesBySku, OrderEventType.CANCELLED);
            case SHIPPED -> changeStatus(event, linesBySku, OrderEventType.SHIPPED);
        };
    }

    private ProcessingResult place(OrderEvent event, Map<String, OrderEventLine> linesBySku) {
        if (!repository.findOrderLines(event.orderId()).isEmpty()) {
            return finish(event, ProcessingOutcome.INVALID, "order has already been placed");
        }

        List<String> sortedSkus = linesBySku.keySet().stream().sorted().toList();
        List<InventoryState> inventory = repository.lockInventory(sortedSkus);
        if (inventory.size() != sortedSkus.size()) {
            return finish(event, ProcessingOutcome.INVALID, missingSkuDetail(sortedSkus, inventory));
        }

        sortedSkus.forEach(sku -> {
            OrderEventLine line = linesBySku.get(sku);
            repository.reserveInventory(line);
            repository.insertOrderLine(event.orderId(), event.sequence(), line);
        });
        return finish(event, ProcessingOutcome.APPLIED, "inventory reserved");
    }

    private ProcessingResult changeStatus(
            OrderEvent event,
            Map<String, OrderEventLine> linesBySku,
            OrderEventType eventType
    ) {
        List<OrderLineState> storedLines = repository.findOrderLines(event.orderId());
        if (!matchesPlacedOrder(linesBySku, storedLines)) {
            return finish(event, ProcessingOutcome.INVALID,
                    "event lines do not match an order whose current status is PLACED");
        }

        List<String> sortedSkus = linesBySku.keySet().stream().sorted().toList();
        List<InventoryState> inventory = repository.lockInventory(sortedSkus);
        if (inventory.size() != sortedSkus.size()) {
            return finish(event, ProcessingOutcome.INVALID, missingSkuDetail(sortedSkus, inventory));
        }

        for (String sku : sortedSkus) {
            OrderEventLine line = linesBySku.get(sku);
            if (eventType == OrderEventType.CANCELLED) {
                repository.releaseInventory(line);
                repository.updateOrderLine(event.orderId(), sku, event.sequence(), "CANCELLED");
            } else {
                repository.shipInventory(line);
                repository.updateOrderLine(event.orderId(), sku, event.sequence(), "SHIPPED");
            }
        }
        return finish(event, ProcessingOutcome.APPLIED,
                eventType == OrderEventType.CANCELLED ? "reservation released" : "inventory shipped");
    }

    private ProcessingResult finish(OrderEvent event, ProcessingOutcome outcome, String detail) {
        repository.markEvent(event.eventId(), outcome.name());
        return new ProcessingResult(event.eventId(), outcome, detail);
    }

    private static Map<String, OrderEventLine> uniqueLines(List<OrderEventLine> lines) {
        Map<String, OrderEventLine> result = new HashMap<>();
        for (OrderEventLine line : lines) {
            if (result.put(line.sku(), line) != null) {
                return null;
            }
        }
        return result;
    }

    private static boolean matchesPlacedOrder(
            Map<String, OrderEventLine> incoming,
            List<OrderLineState> stored
    ) {
        if (incoming.size() != stored.size()) {
            return false;
        }
        return stored.stream().allMatch(line -> {
            OrderEventLine incomingLine = incoming.get(line.sku());
            return incomingLine != null
                    && incomingLine.qty() == line.qty()
                    && "PLACED".equals(line.status());
        });
    }

    private static String missingSkuDetail(List<String> requested, List<InventoryState> found) {
        var foundSkus = found.stream().map(InventoryState::sku).toList();
        var missing = requested.stream().filter(sku -> !foundSkus.contains(sku)).toList();
        return "unknown inventory SKU(s): " + String.join(", ", missing);
    }
}

