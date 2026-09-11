package com.example.orderledger.api;

import com.example.orderledger.store.InventoryState;
import com.example.orderledger.store.LedgerRepository;
import com.example.orderledger.store.OrderLineState;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Comparator;

@RestController
@RequestMapping
public class LedgerController {

    private final LedgerRepository repository;

    public LedgerController(LedgerRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/inventory/{sku}")
    public InventoryState inventory(@PathVariable String sku) {
        return repository.findInventory(sku)
                .orElseThrow(() -> notFound("inventory", sku));
    }

    @GetMapping("/orders/{orderId}")
    public OrderView order(@PathVariable String orderId) {
        var lines = repository.findOrderLines(orderId);
        if (lines.isEmpty()) {
            throw notFound("order", orderId);
        }
        String status = lines.getFirst().status();
        long sequence = lines.stream()
                .map(OrderLineState::lastSequence)
                .max(Comparator.naturalOrder())
                .orElseThrow();
        return new OrderView(orderId, status, sequence, lines);
    }

    private static ResponseStatusException notFound(String resource, String id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, resource + " not found: " + id);
    }
}

