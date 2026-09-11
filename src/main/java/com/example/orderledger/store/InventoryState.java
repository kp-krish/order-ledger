package com.example.orderledger.store;

public record InventoryState(
        String sku,
        int qtyAvailable,
        int qtyReserved,
        boolean oversold,
        long version
) {
}

