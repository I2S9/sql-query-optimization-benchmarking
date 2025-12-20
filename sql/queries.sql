-- SQL Query Optimization Benchmarking - Core Workload Queries
-- This file contains 8-12 representative queries for performance analysis
-- Queries include: JOINs, aggregations, GROUP BY, subqueries, and selective filters

-- Query 1: Orders count by country (JOIN + GROUP BY + Aggregation)
-- Simple join with aggregation and ordering
SELECT 
    c.country, 
    COUNT(*) AS orders_count
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.country
ORDER BY orders_count DESC;

-- Query 2: Total revenue by product category (Multiple JOINs + GROUP BY + Aggregation)
-- Complex join across multiple tables with SUM aggregation
SELECT 
    cat.name AS category_name,
    SUM(oi.subtotal) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS orders_count,
    COUNT(oi.order_item_id) AS items_sold
FROM categories cat
JOIN products p ON p.category_id = cat.category_id
JOIN order_items oi ON oi.product_id = p.product_id
JOIN orders o ON o.order_id = oi.order_id
GROUP BY cat.category_id, cat.name
ORDER BY total_revenue DESC;

-- Query 3: Top customers by total spending (JOIN + Aggregation + ORDER BY + LIMIT)
-- Aggregation with filtering and limiting results
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.country,
    SUM(o.total_amount) AS total_spent,
    COUNT(o.order_id) AS order_count
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.country
HAVING COUNT(o.order_id) >= 2
ORDER BY total_spent DESC
LIMIT 10;

-- Query 4: Average order value by month (Aggregation + Date functions + GROUP BY)
-- Date extraction and aggregation
SELECT 
    DATE_TRUNC('month', o.order_date) AS order_month,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(o.total_amount) AS avg_order_value,
    SUM(o.total_amount) AS total_revenue
FROM orders o
WHERE o.status != 'cancelled'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY order_month DESC;

-- Query 5: Products with low stock in high-revenue categories (Subquery + JOIN + Filter)
-- Non-correlated subquery with selective filtering
SELECT 
    p.product_id,
    p.name AS product_name,
    p.stock_quantity,
    cat.name AS category_name,
    (SELECT SUM(oi.subtotal) 
     FROM order_items oi 
     WHERE oi.product_id = p.product_id) AS total_revenue
FROM products p
JOIN categories cat ON cat.category_id = p.category_id
WHERE p.stock_quantity < 50
  AND (SELECT SUM(oi.subtotal) 
       FROM order_items oi 
       WHERE oi.product_id = p.product_id) > 1000
ORDER BY total_revenue DESC;

-- Query 6: Customers who ordered products from multiple categories (Complex JOIN + GROUP BY + HAVING)
-- Multiple joins with complex grouping condition
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    COUNT(DISTINCT cat.category_id) AS categories_ordered,
    COUNT(DISTINCT o.order_id) AS order_count
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN categories cat ON cat.category_id = p.category_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(DISTINCT cat.category_id) >= 3
ORDER BY categories_ordered DESC, order_count DESC;

-- Query 7: Order items with product and category details (Multiple JOINs + Selective Filter)
-- Complex join with selective filtering on multiple conditions
SELECT 
    oi.order_item_id,
    o.order_id,
    o.order_date,
    p.name AS product_name,
    cat.name AS category_name,
    oi.quantity,
    oi.unit_price,
    oi.subtotal,
    c.country AS customer_country
FROM order_items oi
JOIN orders o ON o.order_id = oi.order_id
JOIN products p ON p.product_id = oi.product_id
JOIN categories cat ON cat.category_id = p.category_id
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '6 months'
  AND oi.quantity >= 3
  AND o.status IN ('shipped', 'delivered')
ORDER BY o.order_date DESC, oi.subtotal DESC;

-- Query 8: Category performance comparison (Aggregation + Subquery + Filter)
-- Subquery for comparison with main query aggregation
SELECT 
    cat.name AS category_name,
    COUNT(DISTINCT p.product_id) AS product_count,
    SUM(oi.subtotal) AS total_revenue,
    AVG(oi.unit_price) AS avg_price,
    (SELECT AVG(oi2.subtotal) 
     FROM order_items oi2 
     JOIN products p2 ON p2.product_id = oi2.product_id 
     JOIN categories cat2 ON cat2.category_id = p2.category_id) AS overall_avg_revenue
FROM categories cat
JOIN products p ON p.category_id = cat.category_id
JOIN order_items oi ON oi.product_id = p.product_id
GROUP BY cat.category_id, cat.name
HAVING SUM(oi.subtotal) > (
    SELECT AVG(category_revenue) 
    FROM (
        SELECT SUM(oi3.subtotal) AS category_revenue
        FROM order_items oi3
        JOIN products p3 ON p3.product_id = oi3.product_id
        GROUP BY p3.category_id
    ) AS category_totals
)
ORDER BY total_revenue DESC;

-- Query 9: Recent orders with customer and product details (JOIN + Date Filter + ORDER BY)
-- Multiple joins with date-based filtering
SELECT 
    o.order_id,
    o.order_date,
    o.status,
    o.total_amount,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.country,
    COUNT(oi.order_item_id) AS item_count
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '3 months'
GROUP BY o.order_id, o.order_date, o.status, o.total_amount, 
         c.first_name, c.last_name, c.country
ORDER BY o.order_date DESC
LIMIT 50;

-- Query 10: Products never ordered (LEFT JOIN + NULL filter + Subquery)
-- Anti-join pattern using LEFT JOIN
SELECT 
    p.product_id,
    p.name AS product_name,
    cat.name AS category_name,
    p.price,
    p.stock_quantity
FROM products p
JOIN categories cat ON cat.category_id = p.category_id
LEFT JOIN order_items oi ON oi.product_id = p.product_id
WHERE oi.order_item_id IS NULL
ORDER BY p.price DESC;

-- Query 11: Customer lifetime value by country (Aggregation + GROUP BY + Filter)
-- Complex aggregation with country grouping
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue,
    AVG(o.total_amount) AS avg_order_value,
    MAX(o.total_amount) AS max_order_value
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
WHERE c.country IN ('United States', 'United Kingdom', 'Germany', 'France', 'Canada')
GROUP BY c.country
ORDER BY total_revenue DESC;

-- Query 12: Monthly sales trend with year-over-year comparison (Date functions + Aggregation + Window functions)
-- Window functions and date manipulation
SELECT 
    DATE_TRUNC('month', o.order_date) AS order_month,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.total_amount) AS monthly_revenue,
    AVG(o.total_amount) AS avg_order_value,
    LAG(SUM(o.total_amount)) OVER (ORDER BY DATE_TRUNC('month', o.order_date)) AS prev_month_revenue,
    SUM(o.total_amount) - LAG(SUM(o.total_amount)) OVER (ORDER BY DATE_TRUNC('month', o.order_date)) AS revenue_change
FROM orders o
WHERE o.status != 'cancelled'
GROUP BY DATE_TRUNC('month', o.order_date)
ORDER BY order_month DESC;

