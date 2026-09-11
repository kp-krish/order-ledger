package com.example.orderledger.api;

import com.example.orderledger.store.OrderLineState;

import java.util.List;

public record OrderView(
        String orderId,
        String status,
        long lastSequence,
        List<OrderLineState> lines
) {
    public OrderView {
        lines = List.copyOf(lines);
    }
}

