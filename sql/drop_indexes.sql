-- SQL Query Optimization Benchmarking - Drop All Indexes
-- This file drops all indexes to create a "no index" baseline

-- Drop indexes on foreign keys
DROP INDEX IF EXISTS idx_products_category_id;
DROP INDEX IF EXISTS idx_orders_customer_id;
DROP INDEX IF EXISTS idx_orders_order_date;
DROP INDEX IF EXISTS idx_order_items_order_id;
DROP INDEX IF EXISTS idx_order_items_product_id;

