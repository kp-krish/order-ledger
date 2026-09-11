CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    event_sequence BIGINT NOT NULL CHECK (event_sequence > 0),
    event_type VARCHAR(32) NOT NULL CHECK (
        event_type IN ('order.placed', 'order.cancelled', 'order.shipped')
    ),
    outcome VARCHAR(16) NOT NULL CHECK (
        outcome IN ('PROCESSING', 'APPLIED', 'STALE', 'INVALID')
    ),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processed_events_order_sequence
    ON processed_events (order_id, event_sequence DESC);

CREATE TABLE inventory (
    sku VARCHAR(100) PRIMARY KEY,
    qty_available INTEGER NOT NULL,
    qty_reserved INTEGER NOT NULL DEFAULT 0 CHECK (qty_reserved >= 0),
    oversold BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT inventory_oversold_matches_quantity
        CHECK (oversold = (qty_available < 0))
);

CREATE TABLE order_lines (
    order_id VARCHAR(100) NOT NULL,
    sku VARCHAR(100) NOT NULL REFERENCES inventory (sku),
    qty INTEGER NOT NULL CHECK (qty > 0),
    status VARCHAR(16) NOT NULL CHECK (
        status IN ('PLACED', 'CANCELLED', 'SHIPPED')
    ),
    last_sequence BIGINT NOT NULL CHECK (last_sequence > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, sku)
);

CREATE INDEX idx_order_lines_status ON order_lines (status);

