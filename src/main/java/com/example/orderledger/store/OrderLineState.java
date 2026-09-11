package com.example.orderledger.store;

public record OrderLineState(
        String orderId,
        String sku,
        int qty,
        String status,
        long lastSequence
) {
}

