package com.example.orderledger.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("order-ledger")
public record OrderLedgerProperties(
        String topic,
        int topicPartitions,
        short topicReplicas
) {
}

