# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - SQL Analytics & Photon Performance
# MAGIC
# MAGIC This notebook demonstrates high-performance SQL analytics using:
# MAGIC - **Photon Engine** for accelerated query execution
# MAGIC - **SQL Warehouse** best practices
# MAGIC - **Materialized Views** for dashboard optimization
# MAGIC - **Query Federation** across schemas
# MAGIC - **Concurrency testing** at scale
# MAGIC
# MAGIC **Target**: Sub-second queries on billion-row tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration & Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG sourcefuse_poc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Photon is Enabled

# COMMAND ----------

# DBTITLE 1,Cell 5
# Check if Photon is enabled
try:
    photon_enabled = spark.conf.get("spark.databricks.photon.enabled")
except Exception:
    photon_enabled = "Auto-enabled on serverless"

print(f"Photon Enabled: {photon_enabled}")
print("\nNote: On serverless clusters, Photon is automatically enabled and optimized.")
print("Manual Photon configuration is not supported on serverless compute.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Complex Analytical Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Sales Trend Analysis (Time Series)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily sales with rolling averages and YoY comparison
# MAGIC WITH daily_metrics AS (
# MAGIC     SELECT 
# MAGIC         transaction_date,
# MAGIC         channel,
# MAGIC         SUM(transaction_amount) as daily_revenue,
# MAGIC         COUNT(*) as transaction_count,
# MAGIC         COUNT(DISTINCT customer_id) as unique_customers
# MAGIC     FROM silver.transactions
# MAGIC     WHERE transaction_type = 'purchase'
# MAGIC     GROUP BY transaction_date, channel
# MAGIC ),
# MAGIC rolling_metrics AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         AVG(daily_revenue) OVER (
# MAGIC             PARTITION BY channel 
# MAGIC             ORDER BY transaction_date 
# MAGIC             ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC         ) as revenue_7day_avg,
# MAGIC         AVG(daily_revenue) OVER (
# MAGIC             PARTITION BY channel 
# MAGIC             ORDER BY transaction_date 
# MAGIC             ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
# MAGIC         ) as revenue_30day_avg,
# MAGIC         LAG(daily_revenue, 365) OVER (
# MAGIC             PARTITION BY channel 
# MAGIC             ORDER BY transaction_date
# MAGIC         ) as revenue_yoy
# MAGIC     FROM daily_metrics
# MAGIC )
# MAGIC SELECT 
# MAGIC     transaction_date,
# MAGIC     channel,
# MAGIC     daily_revenue,
# MAGIC     revenue_7day_avg,
# MAGIC     revenue_30day_avg,
# MAGIC     revenue_yoy,
# MAGIC     ROUND((daily_revenue - revenue_yoy) / NULLIF(revenue_yoy, 0) * 100, 2) as yoy_growth_pct
# MAGIC FROM rolling_metrics
# MAGIC WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC ORDER BY transaction_date DESC, channel
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Customer Cohort Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer cohort retention analysis
# MAGIC WITH customer_cohorts AS (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         DATE_TRUNC('month', MIN(transaction_date)) as cohort_month
# MAGIC     FROM silver.transactions
# MAGIC     WHERE transaction_type = 'purchase'
# MAGIC     GROUP BY customer_id
# MAGIC ),
# MAGIC monthly_activity AS (
# MAGIC     SELECT 
# MAGIC         t.customer_id,
# MAGIC         c.cohort_month,
# MAGIC         DATE_TRUNC('month', t.transaction_date) as activity_month,
# MAGIC         MONTHS_BETWEEN(DATE_TRUNC('month', t.transaction_date), c.cohort_month) as months_since_cohort
# MAGIC     FROM silver.transactions t
# MAGIC     JOIN customer_cohorts c ON t.customer_id = c.customer_id
# MAGIC     WHERE t.transaction_type = 'purchase'
# MAGIC ),
# MAGIC cohort_size AS (
# MAGIC     SELECT cohort_month, COUNT(DISTINCT customer_id) as cohort_customers
# MAGIC     FROM customer_cohorts
# MAGIC     GROUP BY cohort_month
# MAGIC ),
# MAGIC retention AS (
# MAGIC     SELECT 
# MAGIC         m.cohort_month,
# MAGIC         m.months_since_cohort,
# MAGIC         COUNT(DISTINCT m.customer_id) as active_customers
# MAGIC     FROM monthly_activity m
# MAGIC     GROUP BY m.cohort_month, m.months_since_cohort
# MAGIC )
# MAGIC SELECT 
# MAGIC     r.cohort_month,
# MAGIC     cs.cohort_customers,
# MAGIC     r.months_since_cohort,
# MAGIC     r.active_customers,
# MAGIC     ROUND(r.active_customers / cs.cohort_customers * 100, 2) as retention_rate
# MAGIC FROM retention r
# MAGIC JOIN cohort_size cs ON r.cohort_month = cs.cohort_month
# MAGIC WHERE r.months_since_cohort <= 12
# MAGIC   AND r.cohort_month >= DATE_SUB(CURRENT_DATE(), 365)
# MAGIC ORDER BY r.cohort_month, r.months_since_cohort;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Product Affinity Analysis (Market Basket)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find products frequently purchased together
# MAGIC WITH transaction_products AS (
# MAGIC     SELECT DISTINCT
# MAGIC         li.transaction_id,
# MAGIC         li.product_id,
# MAGIC         p.category,
# MAGIC         p.subcategory
# MAGIC     FROM bronze.transaction_line_items li
# MAGIC     JOIN silver.products p ON li.product_id = p.product_id
# MAGIC ),
# MAGIC product_pairs AS (
# MAGIC     SELECT 
# MAGIC         a.product_id as product_a,
# MAGIC         a.category as category_a,
# MAGIC         b.product_id as product_b,
# MAGIC         b.category as category_b,
# MAGIC         COUNT(DISTINCT a.transaction_id) as co_occurrence_count
# MAGIC     FROM transaction_products a
# MAGIC     JOIN transaction_products b 
# MAGIC         ON a.transaction_id = b.transaction_id 
# MAGIC         AND a.product_id < b.product_id
# MAGIC     GROUP BY a.product_id, a.category, b.product_id, b.category
# MAGIC     HAVING COUNT(DISTINCT a.transaction_id) >= 100
# MAGIC ),
# MAGIC product_counts AS (
# MAGIC     SELECT product_id, COUNT(DISTINCT transaction_id) as product_count
# MAGIC     FROM transaction_products
# MAGIC     GROUP BY product_id
# MAGIC )
# MAGIC SELECT 
# MAGIC     pp.product_a,
# MAGIC     pp.category_a,
# MAGIC     pp.product_b,
# MAGIC     pp.category_b,
# MAGIC     pp.co_occurrence_count,
# MAGIC     pa.product_count as product_a_count,
# MAGIC     pb.product_count as product_b_count,
# MAGIC     ROUND(pp.co_occurrence_count / LEAST(pa.product_count, pb.product_count) * 100, 2) as confidence,
# MAGIC     ROUND(pp.co_occurrence_count / (pa.product_count * pb.product_count) * 1000000, 4) as lift
# MAGIC FROM product_pairs pp
# MAGIC JOIN product_counts pa ON pp.product_a = pa.product_id
# MAGIC JOIN product_counts pb ON pp.product_b = pb.product_id
# MAGIC ORDER BY lift DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: Real-time Inventory Optimization

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inventory analysis with demand forecasting signals
# MAGIC WITH recent_sales AS (
# MAGIC     SELECT 
# MAGIC         li.product_id,
# MAGIC         SUM(li.quantity) as units_sold_30d,
# MAGIC         COUNT(DISTINCT li.transaction_id) as transactions_30d,
# MAGIC         AVG(li.quantity) as avg_units_per_transaction
# MAGIC     FROM bronze.transaction_line_items li
# MAGIC     JOIN silver.transactions t ON li.transaction_id = t.transaction_id
# MAGIC     WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC       AND t.transaction_type = 'purchase'
# MAGIC     GROUP BY li.product_id
# MAGIC ),
# MAGIC current_inventory AS (
# MAGIC     SELECT 
# MAGIC         product_id,
# MAGIC         SUM(quantity_available) as total_available,
# MAGIC         SUM(quantity_reserved) as total_reserved,
# MAGIC         AVG(reorder_point) as avg_reorder_point,
# MAGIC         COUNT(DISTINCT warehouse_id) as warehouse_count
# MAGIC     FROM bronze.inventory_snapshots
# MAGIC     WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM bronze.inventory_snapshots)
# MAGIC     GROUP BY product_id
# MAGIC )
# MAGIC SELECT 
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     p.base_price,
# MAGIC     COALESCE(rs.units_sold_30d, 0) as units_sold_30d,
# MAGIC     COALESCE(ci.total_available, 0) as inventory_available,
# MAGIC     ci.warehouse_count,
# MAGIC     CASE 
# MAGIC         WHEN COALESCE(rs.units_sold_30d, 0) > 0 
# MAGIC         THEN ROUND(ci.total_available / (rs.units_sold_30d / 30.0), 1)
# MAGIC         ELSE 999 
# MAGIC     END as days_of_inventory,
# MAGIC     CASE 
# MAGIC         WHEN ci.total_available IS NULL OR ci.total_available = 0 THEN 'OUT_OF_STOCK'
# MAGIC         WHEN ci.total_available < ci.avg_reorder_point THEN 'LOW_STOCK'
# MAGIC         WHEN ci.total_available < rs.units_sold_30d THEN 'REORDER_SOON'
# MAGIC         ELSE 'HEALTHY'
# MAGIC     END as stock_status,
# MAGIC     ROUND(COALESCE(rs.units_sold_30d, 0) / 30.0 * p.base_price * 7, 2) as weekly_revenue_at_risk
# MAGIC FROM silver.products p
# MAGIC LEFT JOIN recent_sales rs ON p.product_id = rs.product_id
# MAGIC LEFT JOIN current_inventory ci ON p.product_id = ci.product_id
# MAGIC WHERE p.is_active = true
# MAGIC ORDER BY weekly_revenue_at_risk DESC
# MAGIC LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Materialized Views for Dashboard Performance

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC %sql
# MAGIC -- Create aggregated table for executive dashboard (materialized views require SQL Warehouse)
# MAGIC CREATE OR REPLACE TABLE gold.mv_executive_summary AS
# MAGIC SELECT 
# MAGIC     DATE_TRUNC('week', transaction_date) as week_start,
# MAGIC     SUM(net_sales) as weekly_revenue,
# MAGIC     SUM(transaction_count) as weekly_transactions,
# MAGIC     SUM(unique_customers) as weekly_customers,
# MAGIC     AVG(avg_transaction_value) as avg_order_value,
# MAGIC     SUM(return_amount) as weekly_returns,
# MAGIC     AVG(return_rate) as avg_return_rate
# MAGIC FROM gold.daily_sales_summary
# MAGIC GROUP BY DATE_TRUNC('week', transaction_date);

# COMMAND ----------

# DBTITLE 1,Cell 17
# MAGIC %sql
# MAGIC -- Create aggregated table for category performance (materialized views require SQL Warehouse)
# MAGIC CREATE OR REPLACE TABLE gold.mv_category_performance AS
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) as product_count,
# MAGIC     SUM(total_units_sold) as total_units,
# MAGIC     SUM(total_revenue) as total_revenue,
# MAGIC     AVG(avg_selling_price) as avg_price,
# MAGIC     SUM(unique_customers) as total_customers
# MAGIC FROM gold.product_performance
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Query Performance Benchmarking

# COMMAND ----------

import time

def benchmark_query(query_name, sql_query, iterations=3):
    """Run a query multiple times and report performance"""
    times = []
    row_count = 0
    
    for i in range(iterations):
        start = time.time()
        df = spark.sql(sql_query)
        row_count = df.count()
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"  Iteration {i+1}: {elapsed:.2f}s ({row_count:,} rows)")
    
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    
    print(f"\n  Summary: avg={avg_time:.2f}s, min={min_time:.2f}s, max={max_time:.2f}s")
    return {"query": query_name, "avg": avg_time, "min": min_time, "max": max_time, "rows": row_count}

# COMMAND ----------

# Define benchmark queries
benchmark_queries = {
    "Simple Aggregation": """
        SELECT channel, COUNT(*), SUM(transaction_amount) 
        FROM silver.transactions 
        GROUP BY channel
    """,
    
    "Date Range Scan": """
        SELECT transaction_date, COUNT(*), SUM(transaction_amount)
        FROM silver.transactions
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY transaction_date
    """,
    
    "Large Join": """
        SELECT c.customer_segment, COUNT(DISTINCT t.customer_id), SUM(t.transaction_amount)
        FROM silver.transactions t
        JOIN silver.customers c ON t.customer_id = c.customer_id
        WHERE t.transaction_type = 'purchase'
        GROUP BY c.customer_segment
    """,
    
    "Complex Multi-Join": """
        SELECT 
            p.category,
            c.customer_segment,
            t.channel,
            COUNT(*) as transactions,
            SUM(li.line_total) as revenue
        FROM bronze.transaction_line_items li
        JOIN silver.transactions t ON li.transaction_id = t.transaction_id
        JOIN silver.products p ON li.product_id = p.product_id
        JOIN silver.customers c ON t.customer_id = c.customer_id
        WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE(), 90)
        GROUP BY p.category, c.customer_segment, t.channel
    """
}

# Run benchmarks
print("=" * 80)
print("QUERY PERFORMANCE BENCHMARK")
print("=" * 80)

results = []
for name, query in benchmark_queries.items():
    print(f"\n{name}:")
    result = benchmark_query(name, query)
    results.append(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Concurrency Testing

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def run_concurrent_query(query_id, query):
    """Execute a query and return timing"""
    thread_name = threading.current_thread().name
    start = time.time()
    try:
        df = spark.sql(query)
        count = df.count()
        elapsed = time.time() - start
        return {"query_id": query_id, "time": elapsed, "rows": count, "status": "success"}
    except Exception as e:
        elapsed = time.time() - start
        return {"query_id": query_id, "time": elapsed, "rows": 0, "status": f"error: {str(e)[:50]}"}

# COMMAND ----------

# Define concurrent query mix
concurrent_queries = [
    "SELECT COUNT(*) FROM silver.transactions",
    "SELECT channel, SUM(transaction_amount) FROM silver.transactions GROUP BY channel",
    "SELECT customer_segment, COUNT(*) FROM silver.customers GROUP BY customer_segment",
    "SELECT category, SUM(total_revenue) FROM gold.product_performance GROUP BY category",
    "SELECT state, COUNT(*) FROM silver.customers WHERE dq_valid_state = true GROUP BY state",
    "SELECT value_segment, AVG(total_spend) FROM gold.customer_360 GROUP BY value_segment",
    "SELECT performance_tier, COUNT(*) FROM gold.store_analytics GROUP BY performance_tier"
]

# Run concurrency test
print("=" * 80)
print("CONCURRENCY TEST - 10 Concurrent Users")
print("=" * 80)

num_users = 10
queries_per_user = 5

all_results = []
with ThreadPoolExecutor(max_workers=num_users) as executor:
    futures = []
    for user in range(num_users):
        for q in range(queries_per_user):
            query = concurrent_queries[(user + q) % len(concurrent_queries)]
            query_id = f"user{user}_q{q}"
            futures.append(executor.submit(run_concurrent_query, query_id, query))
    
    for future in as_completed(futures):
        result = future.result()
        all_results.append(result)
        print(f"  {result['query_id']}: {result['time']:.2f}s - {result['status']}")

# Summary statistics
successful = [r for r in all_results if r['status'] == 'success']
if successful:
    avg_time = sum(r['time'] for r in successful) / len(successful)
    p95_time = sorted([r['time'] for r in successful])[int(len(successful) * 0.95)]
    
    print(f"\nConcurrency Test Summary:")
    print(f"  Total Queries: {len(all_results)}")
    print(f"  Successful: {len(successful)}")
    print(f"  Avg Response Time: {avg_time:.2f}s")
    print(f"  P95 Response Time: {p95_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Warehouse Best Practices Summary
# MAGIC
# MAGIC 1. **Enable Photon**: Provides 2-8x speedup for most analytical queries
# MAGIC 2. **Use Z-ORDER**: On frequently filtered columns (customer_id, transaction_date)
# MAGIC 3. **Partition wisely**: By date columns for time-series data
# MAGIC 4. **Materialized Views**: For frequently accessed dashboard queries
# MAGIC 5. **Compute Statistics**: `ANALYZE TABLE` for better query planning
# MAGIC 6. **Auto-scaling**: Configure warehouse to scale 2-8 clusters for concurrency
# MAGIC 7. **Query Caching**: Leverage result cache for repeated queries