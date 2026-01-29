-- =============================================================================
-- DATABRICKS RETAIL POC - SQL SCRIPTS
-- =============================================================================

-- =============================================================================
-- 1. TABLE OPTIMIZATION SCRIPTS
-- =============================================================================

-- Optimize Bronze Tables
OPTIMIZE sourcefuse_poc.bronze.transactions ZORDER BY (customer_id, store_id);
OPTIMIZE sourcefuse_poc.bronze.transaction_line_items ZORDER BY (product_id, transaction_id);
OPTIMIZE sourcefuse_poc.bronze.customers ZORDER BY (state, customer_segment);
OPTIMIZE sourcefuse_poc.bronze.inventory_snapshots ZORDER BY (product_id, warehouse_id);

-- Optimize Silver Tables
OPTIMIZE sourcefuse_poc.silver.transactions ZORDER BY (customer_id, transaction_date);
OPTIMIZE sourcefuse_poc.silver.customers ZORDER BY (state, customer_segment, tenure_category);
OPTIMIZE sourcefuse_poc.silver.products ZORDER BY (category, subcategory);

-- Optimize Gold Tables
OPTIMIZE sourcefuse_poc.gold.daily_sales_summary ZORDER BY (transaction_date, channel);
OPTIMIZE sourcefuse_poc.gold.customer_360 ZORDER BY (value_segment, customer_segment);
OPTIMIZE sourcefuse_poc.gold.product_performance ZORDER BY (category, performance_class);
OPTIMIZE sourcefuse_poc.gold.store_analytics ZORDER BY (state, performance_tier);


-- =============================================================================
-- 2. STATISTICS COMPUTATION
-- =============================================================================

ANALYZE TABLE sourcefuse_poc.silver.transactions COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE sourcefuse_poc.silver.customers COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE sourcefuse_poc.silver.products COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE sourcefuse_poc.gold.daily_sales_summary COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE sourcefuse_poc.gold.customer_360 COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE sourcefuse_poc.gold.product_performance COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE sourcefuse_poc.gold.store_analytics COMPUTE STATISTICS FOR ALL COLUMNS;


-- =============================================================================
-- 3. VACUUM SCRIPTS (Weekly Maintenance)
-- =============================================================================

-- Vacuum Bronze (retain 7 days)
VACUUM sourcefuse_poc.bronze.transactions RETAIN 168 HOURS;
VACUUM sourcefuse_poc.bronze.transaction_line_items RETAIN 168 HOURS;
VACUUM sourcefuse_poc.bronze.customers RETAIN 168 HOURS;
VACUUM sourcefuse_poc.bronze.inventory_snapshots RETAIN 168 HOURS;
VACUUM sourcefuse_poc.bronze.clickstream_sample RETAIN 168 HOURS;

-- Vacuum Silver (retain 14 days)
VACUUM sourcefuse_poc.silver.transactions RETAIN 336 HOURS;
VACUUM sourcefuse_poc.silver.customers RETAIN 336 HOURS;
VACUUM sourcefuse_poc.silver.products RETAIN 336 HOURS;

-- Vacuum Gold (retain 30 days for compliance)
VACUUM sourcefuse_poc.gold.daily_sales_summary RETAIN 720 HOURS;
VACUUM sourcefuse_poc.gold.customer_360 RETAIN 720 HOURS;
VACUUM sourcefuse_poc.gold.product_performance RETAIN 720 HOURS;
VACUUM sourcefuse_poc.gold.store_analytics RETAIN 720 HOURS;


-- =============================================================================
-- 4. MATERIALIZED VIEW REFRESH
-- =============================================================================

REFRESH MATERIALIZED VIEW sourcefuse_poc.gold.mv_executive_summary;
REFRESH MATERIALIZED VIEW sourcefuse_poc.gold.mv_category_performance;
REFRESH MATERIALIZED VIEW sourcefuse_poc.gold.mv_geographic_summary;


-- =============================================================================
-- 5. DATA QUALITY QUERIES
-- =============================================================================

-- Check for null customer_ids in transactions
SELECT 
    'silver.transactions' as table_name,
    'NULL customer_id' as check_type,
    COUNT(*) as violation_count
FROM sourcefuse_poc.silver.transactions
WHERE customer_id IS NULL;

-- Check for invalid transaction amounts
SELECT 
    'silver.transactions' as table_name,
    'Negative amount' as check_type,
    COUNT(*) as violation_count
FROM sourcefuse_poc.silver.transactions
WHERE transaction_amount < 0;

-- Check for orphaned line items
SELECT 
    'bronze.transaction_line_items' as table_name,
    'Orphaned records' as check_type,
    COUNT(*) as violation_count
FROM sourcefuse_poc.bronze.transaction_line_items li
LEFT JOIN sourcefuse_poc.silver.transactions t ON li.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Customer data quality summary
SELECT 
    'silver.customers' as table_name,
    ROUND(AVG(dq_score), 2) as avg_dq_score,
    SUM(CASE WHEN dq_valid_email THEN 1 ELSE 0 END) as valid_emails,
    SUM(CASE WHEN dq_valid_state THEN 1 ELSE 0 END) as valid_states,
    COUNT(*) as total_records
FROM sourcefuse_poc.silver.customers;


-- =============================================================================
-- 6. BUSINESS KPI QUERIES
-- =============================================================================

-- Daily Revenue Summary
SELECT 
    transaction_date,
    SUM(net_sales) as total_revenue,
    SUM(transaction_count) as total_transactions,
    SUM(unique_customers) as unique_customers,
    ROUND(SUM(net_sales) / SUM(transaction_count), 2) as avg_order_value
FROM sourcefuse_poc.gold.daily_sales_summary
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- Customer Segment Performance
SELECT 
    value_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(total_spend), 2) as avg_spend,
    ROUND(AVG(avg_order_value), 2) as avg_order_value,
    ROUND(AVG(rfm_score), 2) as avg_rfm_score,
    ROUND(AVG(return_rate), 2) as avg_return_rate
FROM sourcefuse_poc.gold.customer_360
GROUP BY value_segment
ORDER BY avg_spend DESC;

-- Top Performing Products
SELECT 
    product_id,
    product_name,
    category,
    total_revenue,
    total_units_sold,
    unique_customers,
    performance_class,
    category_revenue_rank
FROM sourcefuse_poc.gold.product_performance
WHERE performance_class IN ('Star', 'Strong')
ORDER BY total_revenue DESC
LIMIT 100;

-- Store Performance by Region
SELECT 
    state,
    COUNT(*) as store_count,
    ROUND(SUM(total_revenue), 2) as total_revenue,
    ROUND(AVG(revenue_per_sqft), 2) as avg_revenue_per_sqft,
    ROUND(AVG(avg_daily_revenue), 2) as avg_daily_revenue
FROM sourcefuse_poc.gold.store_analytics
GROUP BY state
ORDER BY total_revenue DESC;


-- =============================================================================
-- 7. TIME TRAVEL QUERIES (Delta Lake Feature)
-- =============================================================================

-- View table history
DESCRIBE HISTORY sourcefuse_poc.gold.customer_360 LIMIT 10;

-- Query data as of specific version
-- SELECT * FROM sourcefuse_poc.gold.customer_360 VERSION AS OF 5;

-- Query data as of specific timestamp
-- SELECT * FROM sourcefuse_poc.gold.customer_360 TIMESTAMP AS OF '2024-01-15 10:00:00';

-- Restore to previous version
-- RESTORE sourcefuse_poc.gold.customer_360 TO VERSION AS OF 5;


-- =============================================================================
-- 8. CHANGE DATA FEED QUERIES
-- =============================================================================

-- Read changes from customer_360 (requires CDF enabled)
-- SELECT * FROM table_changes('sourcefuse_poc.gold.customer_360', 1)
-- WHERE _change_type IN ('insert', 'update_postimage');


-- =============================================================================
-- 9. PERFORMANCE MONITORING QUERIES
-- =============================================================================

-- Table sizes and row counts
SELECT 
    table_catalog,
    table_schema,
    table_name,
    ROUND(SUM(size_in_bytes) / 1024 / 1024 / 1024, 2) as size_gb
FROM system.information_schema.tables
WHERE table_catalog = 'sourcefuse_poc'
GROUP BY table_catalog, table_schema, table_name
ORDER BY size_gb DESC;

-- Recent query performance (if query history enabled)
-- SELECT 
--     query_id,
--     query_text,
--     execution_status,
--     duration / 1000 as duration_seconds,
--     rows_produced
-- FROM system.query.history
-- WHERE start_time >= DATE_SUB(CURRENT_TIMESTAMP(), 1)
-- ORDER BY duration DESC
-- LIMIT 20;
