package com.example.orderledger.store;

import com.example.orderledger.event.OrderEvent;
import com.example.orderledger.event.OrderEventLine;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

@Repository
public class LedgerRepository {

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;

    public LedgerRepository(JdbcTemplate jdbcTemplate, NamedParameterJdbcTemplate namedJdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedJdbcTemplate = namedJdbcTemplate;
    }

    public void lockOrder(String orderId) {
        jdbcTemplate.query(
                "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))",
                resultSet -> null,
                orderId
        );
    }

    public void insertEvent(OrderEvent event) {
        jdbcTemplate.update("""
                INSERT INTO processed_events
                    (event_id, order_id, event_sequence, event_type, outcome)
                VALUES (?, ?, ?, ?, 'PROCESSING')
                """,
                event.eventId(), event.orderId(), event.sequence(), event.eventType());
    }

    public OptionalLong findLastAppliedSequence(String orderId, UUID currentEventId) {
        Long value = jdbcTemplate.queryForObject("""
                SELECT MAX(event_sequence)
                FROM processed_events
                WHERE order_id = ? AND event_id <> ? AND outcome = 'APPLIED'
                """, Long.class, orderId, currentEventId);
        return value == null ? OptionalLong.empty() : OptionalLong.of(value);
    }

    public void markEvent(UUID eventId, String outcome) {
        jdbcTemplate.update(
                "UPDATE processed_events SET outcome = ?, processed_at = CURRENT_TIMESTAMP WHERE event_id = ?",
                outcome,
                eventId
        );
    }

    public List<InventoryState> lockInventory(List<String> sortedSkus) {
        return namedJdbcTemplate.query("""
                SELECT sku, qty_available, qty_reserved, oversold, version
                FROM inventory
                WHERE sku IN (:skus)
                ORDER BY sku
                FOR UPDATE
                """,
                new MapSqlParameterSource("skus", sortedSkus),
                LedgerRepository::mapInventory);
    }

    public Optional<InventoryState> findInventory(String sku) {
        return jdbcTemplate.query("""
                SELECT sku, qty_available, qty_reserved, oversold, version
                FROM inventory
                WHERE sku = ?
                """, LedgerRepository::mapInventory, sku).stream().findFirst();
    }

    public List<OrderLineState> findOrderLines(String orderId) {
        return jdbcTemplate.query("""
                SELECT order_id, sku, qty, status, last_sequence
                FROM order_lines
                WHERE order_id = ?
                ORDER BY sku
                """, LedgerRepository::mapOrderLine, orderId);
    }

    public void reserveInventory(OrderEventLine line) {
        jdbcTemplate.update("""
                UPDATE inventory
                SET qty_available = qty_available - ?,
                    qty_reserved = qty_reserved + ?,
                    oversold = (qty_available - ? < 0),
                    version = version + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sku = ?
                """, line.qty(), line.qty(), line.qty(), line.sku());
    }

    public void releaseInventory(OrderEventLine line) {
        jdbcTemplate.update("""
                UPDATE inventory
                SET qty_available = qty_available + ?,
                    qty_reserved = qty_reserved - ?,
                    oversold = (qty_available + ? < 0),
                    version = version + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sku = ?
                """, line.qty(), line.qty(), line.qty(), line.sku());
    }

    public void shipInventory(OrderEventLine line) {
        jdbcTemplate.update("""
                UPDATE inventory
                SET qty_reserved = qty_reserved - ?,
                    version = version + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sku = ?
                """, line.qty(), line.sku());
    }

    public void insertOrderLine(String orderId, long sequence, OrderEventLine line) {
        jdbcTemplate.update("""
                INSERT INTO order_lines (order_id, sku, qty, status, last_sequence)
                VALUES (?, ?, ?, 'PLACED', ?)
                """, orderId, line.sku(), line.qty(), sequence);
    }

    public void updateOrderLine(String orderId, String sku, long sequence, String status) {
        jdbcTemplate.update("""
                UPDATE order_lines
                SET status = ?, last_sequence = ?, updated_at = CURRENT_TIMESTAMP
                WHERE order_id = ? AND sku = ?
                """, status, sequence, orderId, sku);
    }

    private static InventoryState mapInventory(ResultSet rs, int rowNumber) throws SQLException {
        return new InventoryState(
                rs.getString("sku"),
                rs.getInt("qty_available"),
                rs.getInt("qty_reserved"),
                rs.getBoolean("oversold"),
                rs.getLong("version")
        );
    }

    private static OrderLineState mapOrderLine(ResultSet rs, int rowNumber) throws SQLException {
        return new OrderLineState(
                rs.getString("order_id"),
                rs.getString("sku"),
                rs.getInt("qty"),
                rs.getString("status"),
                rs.getLong("last_sequence")
        );
    }
}
