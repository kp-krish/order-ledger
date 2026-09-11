package com.example.orderledger;

import com.example.orderledger.event.OrderEvent;
import com.example.orderledger.event.OrderEventLine;
import com.example.orderledger.processing.OrderEventProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(properties = "order-ledger.topic-partitions=3")
class OrderLedgerIntegrationTest {

    @Container
    static final PostgreSQLContainer postgres = new PostgreSQLContainer(
            DockerImageName.parse("postgres:17.6-alpine"));

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.9.1"));

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private MeterRegistry meterRegistry;
    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;
    @Autowired
    private OrderEventProcessor processor;

    @BeforeEach
    void resetDatabase() {
        startConsumer();
        jdbcTemplate.update("DELETE FROM order_lines");
        jdbcTemplate.update("DELETE FROM processed_events");
        jdbcTemplate.update("DELETE FROM inventory");
        jdbcTemplate.update("""
                INSERT INTO inventory (sku, qty_available, qty_reserved, oversold, version)
                VALUES ('SKU-001', 1000, 0, FALSE, 0), ('SKU-002', 1000, 0, FALSE, 0)
                """);
    }

    @Test
    void duplicateDeliveryIsRejectedWithoutDoubleApplying() throws Exception {
        OrderEvent placed = event("duplicate-order", 1, "order.placed",
                List.of(new OrderEventLine("SKU-001", 5)));
        double duplicateCount = eventCount("duplicate");

        send(placed);
        awaitProcessedEvents(1);
        send(placed);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(eventCount("duplicate")).isEqualTo(duplicateCount + 1));
        assertInventory("SKU-001", 995, 5, 1);
        assertThat(processedEventCount()).isEqualTo(1);
    }

    @Test
    void lowerSequenceIsRecordedAsStaleAndDoesNotMutateState() throws Exception {
        var lines = List.of(new OrderEventLine("SKU-001", 5));
        OrderEvent placed = event("ordered-order", 1, "order.placed", lines);
        OrderEvent shipped = event("ordered-order", 3, "order.shipped", lines);
        OrderEvent lateCancellation = event("ordered-order", 2, "order.cancelled", lines);

        send(placed);
        send(shipped);
        send(lateCancellation);

        awaitProcessedEvents(3);
        assertInventory("SKU-001", 995, 0, 2);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT status FROM order_lines WHERE order_id = 'ordered-order'", String.class))
                .isEqualTo("SHIPPED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT outcome FROM processed_events WHERE event_id = ?",
                String.class,
                lateCancellation.eventId())).isEqualTo("STALE");
    }

    @Test
    void consumerRestartRecoversLagWithoutLossOrDoubleApply() throws Exception {
        OrderEvent first = event("restart-order-0", 1, "order.placed",
                List.of(new OrderEventLine("SKU-001", 1)));
        send(first);
        awaitProcessedEvents(1);

        stopConsumer();
        double duplicateCount = eventCount("duplicate");
        send(first);
        for (int index = 1; index <= 5; index++) {
            send(event("restart-order-" + index, 1, "order.placed",
                    List.of(new OrderEventLine("SKU-001", 1))));
        }
        assertThat(processedEventCount()).isEqualTo(1);

        startConsumer();

        awaitProcessedEvents(6);
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(eventCount("duplicate")).isEqualTo(duplicateCount + 1));
        assertInventory("SKU-001", 994, 6, 6);
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_lines WHERE order_id LIKE 'restart-order-%'",
                Integer.class)).isEqualTo(6);
    }

    @Test
    void oppositeInputOrdersCompleteWithConsistentSkuLocking() throws Exception {
        List<Callable<Void>> tasks = new ArrayList<>();
        for (int index = 0; index < 20; index++) {
            int taskNumber = index;
            tasks.add(() -> {
                List<OrderEventLine> lines = taskNumber % 2 == 0
                        ? List.of(new OrderEventLine("SKU-001", 1), new OrderEventLine("SKU-002", 1))
                        : List.of(new OrderEventLine("SKU-002", 1), new OrderEventLine("SKU-001", 1));
                processor.process(event("concurrent-order-" + taskNumber, 1, "order.placed", lines));
                return null;
            });
        }

        try (var executor = java.util.concurrent.Executors.newFixedThreadPool(8)) {
            var futures = executor.invokeAll(tasks);
            for (var future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        }

        assertInventory("SKU-001", 980, 20, 20);
        assertInventory("SKU-002", 980, 20, 20);
        assertThat(processedEventCount()).isEqualTo(20);
    }

    private void send(OrderEvent event) throws Exception {
        kafkaTemplate.send("orders.events", event.orderId(), objectMapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);
    }

    private void awaitProcessedEvents(int expected) {
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(processedEventCount()).isEqualTo(expected));
    }

    private int processedEventCount() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM processed_events", Integer.class);
    }

    private double eventCount(String outcome) {
        return meterRegistry.get("order.ledger.events").tag("outcome", outcome).counter().count();
    }

    private void assertInventory(String sku, int available, int reserved, long version) {
        var values = jdbcTemplate.queryForMap(
                "SELECT qty_available, qty_reserved, version FROM inventory WHERE sku = ?", sku);
        assertThat(values.get("qty_available")).isEqualTo(available);
        assertThat(values.get("qty_reserved")).isEqualTo(reserved);
        assertThat(values.get("version")).isEqualTo(version);
    }

    private void stopConsumer() {
        listenerRegistry.getListenerContainers().forEach(container -> container.stop());
        await().atMost(Duration.ofSeconds(10)).until(() -> listenerRegistry.getListenerContainers()
                .stream().noneMatch(container -> container.isRunning()));
    }

    private void startConsumer() {
        listenerRegistry.getListenerContainers().forEach(container -> {
            if (!container.isRunning()) {
                container.start();
            }
        });
        await().atMost(Duration.ofSeconds(10)).until(() -> listenerRegistry.getListenerContainers()
                .stream().allMatch(container -> container.isRunning()));
    }

    private static OrderEvent event(
            String orderId,
            long sequence,
            String type,
            List<OrderEventLine> lines
    ) {
        return new OrderEvent(UUID.randomUUID(), 1, type, orderId, sequence, Instant.now(), lines);
    }
}

