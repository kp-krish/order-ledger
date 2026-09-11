package com.example.orderledger.messaging;

import com.example.orderledger.event.OrderEvent;
import com.example.orderledger.processing.OrderEventProcessor;
import com.example.orderledger.processing.ProcessingOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class OrderEventsListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderEventsListener.class);

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final OrderEventProcessor processor;
    private final Map<ProcessingOutcome, Counter> outcomeCounters;
    private final Counter duplicateCounter;
    private final Counter rejectedCounter;

    public OrderEventsListener(
            ObjectMapper objectMapper,
            Validator validator,
            OrderEventProcessor processor,
            MeterRegistry meterRegistry
    ) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.processor = processor;
        this.outcomeCounters = new EnumMap<>(ProcessingOutcome.class);
        for (ProcessingOutcome outcome : ProcessingOutcome.values()) {
            outcomeCounters.put(outcome, eventCounter(meterRegistry, outcome.name().toLowerCase()));
        }
        this.duplicateCounter = eventCounter(meterRegistry, "duplicate");
        this.rejectedCounter = eventCounter(meterRegistry, "rejected");
    }

    @KafkaListener(topics = "${order-ledger.topic}")
    public void onEvent(ConsumerRecord<String, String> record) {
        OrderEvent event = deserialize(record.value());
        validate(record.key(), event);

        try {
            var result = processor.process(event);
            outcomeCounters.get(result.outcome()).increment();
            LOGGER.info("eventId={} orderId={} sequence={} outcome={} detail={}",
                    event.eventId(), event.orderId(), event.sequence(), result.outcome(), result.detail());
        } catch (DuplicateKeyException duplicate) {
            duplicateCounter.increment();
            LOGGER.info("Ignoring duplicate eventId={} orderId={}", event.eventId(), event.orderId());
        }
    }

    private OrderEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, OrderEvent.class);
        } catch (JacksonException exception) {
            rejectedCounter.increment();
            throw new NonRetryableEventException("Payload is not a valid order event", exception);
        }
    }

    private void validate(String key, OrderEvent event) {
        var violations = validator.validate(event);
        if (!violations.isEmpty()) {
            rejectedCounter.increment();
            String detail = violations.stream()
                    .map(OrderEventsListener::describeViolation)
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new NonRetryableEventException("Event validation failed: " + detail);
        }
        if (!event.orderId().equals(key)) {
            rejectedCounter.increment();
            throw new NonRetryableEventException("Kafka key must equal orderId");
        }
    }

    private static String describeViolation(ConstraintViolation<OrderEvent> violation) {
        return violation.getPropertyPath() + " " + violation.getMessage();
    }

    private static Counter eventCounter(MeterRegistry meterRegistry, String outcome) {
        return Counter.builder("order.ledger.events")
                .description("Order events observed by processing outcome")
                .tag("outcome", outcome)
                .register(meterRegistry);
    }
}

