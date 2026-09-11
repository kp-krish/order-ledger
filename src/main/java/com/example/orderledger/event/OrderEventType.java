package com.example.orderledger.event;

import java.util.Arrays;

public enum OrderEventType {
    PLACED("order.placed"),
    CANCELLED("order.cancelled"),
    SHIPPED("order.shipped");

    private final String wireValue;

    OrderEventType(String wireValue) {
        this.wireValue = wireValue;
    }

    public String wireValue() {
        return wireValue;
    }

    public static OrderEventType fromWireValue(String value) {
        return Arrays.stream(values())
                .filter(type -> type.wireValue.equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported event type: " + value));
    }
}

