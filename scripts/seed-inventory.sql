INSERT INTO inventory (sku, qty_available, qty_reserved, oversold, version)
VALUES
    ('SKU-001', 10000, 0, FALSE, 0),
    ('SKU-002', 10000, 0, FALSE, 0),
    ('SKU-003', 10000, 0, FALSE, 0),
    ('SKU-004', 10000, 0, FALSE, 0),
    ('SKU-005', 10000, 0, FALSE, 0)
ON CONFLICT (sku) DO NOTHING;

