-- SQL Query Optimization Benchmarking - Drop All Indexes
-- This file drops all indexes to create a "no index" baseline

-- Drop indexes on foreign keys
DROP INDEX IF EXISTS idx_products_category_id;
DROP INDEX IF EXISTS idx_orders_customer_id;
DROP INDEX IF EXISTS idx_order_items_order_id;
DROP INDEX IF EXISTS idx_order_items_product_id;

-- Drop indexes on date columns
DROP INDEX IF EXISTS idx_orders_order_date;

-- Drop indexes on filter columns
DROP INDEX IF EXISTS idx_orders_status;
DROP INDEX IF EXISTS idx_products_stock_quantity;
DROP INDEX IF EXISTS idx_customers_country;
DROP INDEX IF EXISTS idx_order_items_quantity;

-- Drop indexes on sort columns
DROP INDEX IF EXISTS idx_order_items_subtotal;
DROP INDEX IF EXISTS idx_products_price;

-- Drop composite indexes
DROP INDEX IF EXISTS idx_orders_status_date;
DROP INDEX IF EXISTS idx_orders_customer_date;
