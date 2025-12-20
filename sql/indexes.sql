-- SQL Query Optimization Benchmarking - Index Definitions
-- This file contains index definitions for performance optimization

-- Indexes on foreign keys (for JOIN operations)
CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);

-- Indexes on date columns (for date filtering and sorting)
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);

-- Indexes on filter columns (for WHERE clauses)
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_products_stock_quantity ON products(stock_quantity);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country);
CREATE INDEX IF NOT EXISTS idx_order_items_quantity ON order_items(quantity);

-- Indexes on sort columns (for ORDER BY)
CREATE INDEX IF NOT EXISTS idx_order_items_subtotal ON order_items(subtotal);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);

-- Composite indexes for common query patterns
-- For queries filtering by status and date (Query 4, 7, 12)
CREATE INDEX IF NOT EXISTS idx_orders_status_date ON orders(status, order_date);

-- For queries joining customer and filtering by date (Query 9)
CREATE INDEX IF NOT EXISTS idx_orders_customer_date ON orders(customer_id, order_date);
