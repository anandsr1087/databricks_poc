# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Batch ETL Pipeline (Medallion Architecture)
# MAGIC 
# MAGIC This notebook demonstrates heavy batch processing using:
# MAGIC - **Bronze → Silver → Gold** medallion architecture
# MAGIC - **Delta Lake** optimizations (OPTIMIZE, Z-ORDER, VACUUM)
# MAGIC - **Change Data Feed** for incremental processing
# MAGIC - **Merge (Upsert)** operations at scale
# MAGIC - **Data quality** validation
# MAGIC 
# MAGIC **Target**: Process 100M+ rows with complex transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CONFIG = {
    "catalog": "sourcefuse_poc",
    "bronze_schema": "bronze",
    "silver_schema": "silver",
    "gold_schema": "gold"
}

spark.sql(f"USE CATALOG {CONFIG['database']}")

# Enable optimizations
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze → Silver: Data Cleansing & Standardization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Customers - Cleansed & Deduplicated

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read bronze customers
bronze_customers = spark.table(f"{CONFIG['database']}.customers")

# Data quality rules and transformations
silver_customers = (
    bronze_customers
    # Standardize text fields
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("first_name", F.initcap(F.trim("first_name")))
    .withColumn("last_name", F.initcap(F.trim("last_name")))
    .withColumn("state", F.upper(F.trim("state")))
    
    # Validate and clean phone numbers
    .withColumn("phone_cleaned", 
        F.regexp_replace("phone", "[^0-9]", ""))
    .withColumn("phone", 
        F.when(F.length("phone_cleaned") == 11, F.col("phone_cleaned"))
         .when(F.length("phone_cleaned") == 10, F.concat(F.lit("1"), F.col("phone_cleaned")))
         .otherwise(None))
    
    # Data quality flags
    .withColumn("dq_valid_email", 
        F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
    .withColumn("dq_valid_state", 
        F.col("state").isin(["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", 
                            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]))
    .withColumn("dq_score", 
        (F.col("dq_valid_email").cast("int") + F.col("dq_valid_state").cast("int")) / 2 * 100)
    
    # Customer tenure calculation
    .withColumn("tenure_days", 
        F.datediff(F.current_date(), "registration_date"))
    .withColumn("tenure_category",
        F.when(F.col("tenure_days") < 90, "New")
         .when(F.col("tenure_days") < 365, "Established")
         .when(F.col("tenure_days") < 730, "Loyal")
         .otherwise("VIP"))
    
    # Recency calculation
    .withColumn("days_since_purchase", 
        F.datediff(F.current_date(), "last_purchase_date"))
    .withColumn("recency_category",
        F.when(F.col("days_since_purchase") < 30, "Active")
         .when(F.col("days_since_purchase") < 90, "Recent")
         .when(F.col("days_since_purchase") < 180, "Lapsed")
         .otherwise("Dormant"))
    
    # Add processing metadata
    .withColumn("silver_processed_at", F.current_timestamp())
    .withColumn("silver_batch_id", F.lit(F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")))
    
    .drop("phone_cleaned")
)

# Write to silver with merge
silver_customers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['database']}.customers")

# Data quality summary
dq_summary = silver_customers.agg(
    F.count("*").alias("total_records"),
    F.sum(F.col("dq_valid_email").cast("int")).alias("valid_emails"),
    F.sum(F.col("dq_valid_state").cast("int")).alias("valid_states"),
    F.avg("dq_score").alias("avg_dq_score")
).collect()[0]

print(f"✅ Silver Customers Created")
print(f"   Total Records: {dq_summary['total_records']:,}")
print(f"   Valid Emails: {dq_summary['valid_emails']:,} ({dq_summary['valid_emails']/dq_summary['total_records']*100:.1f}%)")
print(f"   Valid States: {dq_summary['valid_states']:,} ({dq_summary['valid_states']/dq_summary['total_records']*100:.1f}%)")
print(f"   Avg DQ Score: {dq_summary['avg_dq_score']:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Transactions - Enriched & Validated

# COMMAND ----------

# Read bronze transactions
bronze_transactions = spark.table(f"{CONFIG['database']}.transactions")

# Read line items for aggregation
bronze_line_items = spark.table(f"{CONFIG['database']}.transaction_line_items")

# Aggregate line items to transaction level
transaction_totals = (
    bronze_line_items
    .groupBy("transaction_id")
    .agg(
        F.sum("line_total").alias("calculated_total"),
        F.sum("quantity").alias("total_items"),
        F.count("*").alias("line_item_count"),
        F.avg("discount_percent").alias("avg_discount"),
        F.max("discount_percent").alias("max_discount")
    )
)

# Join and enrich transactions
silver_transactions = (
    bronze_transactions
    .join(transaction_totals, "transaction_id", "left")
    
    # Calculate derived fields
    .withColumn("transaction_amount", F.coalesce("calculated_total", F.lit(0)))
    .withColumn("transaction_hour", F.hour("transaction_timestamp"))
    .withColumn("transaction_day_of_week", F.dayofweek("transaction_date"))
    .withColumn("transaction_week", F.weekofyear("transaction_date"))
    .withColumn("transaction_month", F.month("transaction_date"))
    .withColumn("transaction_quarter", F.quarter("transaction_date"))
    .withColumn("transaction_year", F.year("transaction_date"))
    
    # Time-based categorization
    .withColumn("time_of_day",
        F.when(F.col("transaction_hour").between(6, 11), "Morning")
         .when(F.col("transaction_hour").between(12, 17), "Afternoon")
         .when(F.col("transaction_hour").between(18, 21), "Evening")
         .otherwise("Night"))
    
    .withColumn("is_weekend", 
        F.col("transaction_day_of_week").isin([1, 7]))
    
    # Amount categorization
    .withColumn("amount_bucket",
        F.when(F.col("transaction_amount") < 25, "Small (<$25)")
         .when(F.col("transaction_amount") < 100, "Medium ($25-$100)")
         .when(F.col("transaction_amount") < 500, "Large ($100-$500)")
         .otherwise("XLarge ($500+)"))
    
    # Data quality flags
    .withColumn("dq_has_items", F.col("line_item_count") > 0)
    .withColumn("dq_valid_amount", F.col("transaction_amount") > 0)
    .withColumn("dq_consistent_items", F.col("line_item_count") == F.col("num_items"))
    
    # Processing metadata
    .withColumn("silver_processed_at", F.current_timestamp())
    
    .drop("calculated_total")
)

# Write to silver (partitioned for performance)
silver_transactions.repartition(200, "year_month") \
    .write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['database']}.transactions")

print(f"✅ Silver Transactions Created: {silver_transactions.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Products - Enhanced Catalog

# COMMAND ----------

# Read bronze products
bronze_products = spark.table(f"{CONFIG['database']}.products")

# Read transaction line items for product metrics
bronze_line_items = spark.table(f"{CONFIG['database']}.transaction_line_items")

# Calculate product performance metrics
product_metrics = (
    bronze_line_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("times_sold"),
        F.sum("quantity").alias("units_sold"),
        F.sum("line_total").alias("total_revenue"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.avg("discount_percent").alias("avg_discount_given"),
        F.countDistinct("transaction_id").alias("unique_transactions")
    )
)

# Enhance products with metrics
silver_products = (
    bronze_products
    .join(product_metrics, "product_id", "left")
    
    # Fill nulls for products with no sales
    .fillna({
        "times_sold": 0, 
        "units_sold": 0, 
        "total_revenue": 0.0,
        "unique_transactions": 0
    })
    
    # Calculate derived metrics
    .withColumn("profit_margin", 
        ((F.col("base_price") - F.col("cost_price")) / F.col("base_price") * 100).cast("decimal(5,2)"))
    .withColumn("revenue_per_transaction", 
        F.when(F.col("unique_transactions") > 0, 
               F.col("total_revenue") / F.col("unique_transactions"))
         .otherwise(0))
    .withColumn("avg_units_per_transaction",
        F.when(F.col("unique_transactions") > 0,
               F.col("units_sold") / F.col("unique_transactions"))
         .otherwise(0))
    
    # Product tier based on revenue
    .withColumn("revenue_tier",
        F.when(F.col("total_revenue") >= 100000, "Platinum")
         .when(F.col("total_revenue") >= 50000, "Gold")
         .when(F.col("total_revenue") >= 10000, "Silver")
         .when(F.col("total_revenue") > 0, "Bronze")
         .otherwise("New"))
    
    # Price positioning
    .withColumn("price_tier",
        F.when(F.col("base_price") >= 500, "Premium")
         .when(F.col("base_price") >= 100, "Mid-Range")
         .when(F.col("base_price") >= 25, "Value")
         .otherwise("Budget"))
    
    # Processing metadata
    .withColumn("silver_processed_at", F.current_timestamp())
)

silver_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['database']}.products")

print(f"✅ Silver Products Created: {silver_products.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver → Gold: Business Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: Daily Sales Summary

# COMMAND ----------

silver_transactions = spark.table(f"{CONFIG['database']}.transactions")
silver_products = spark.table(f"{CONFIG['database']}.products")
bronze_line_items = spark.table(f"{CONFIG['database']}.transaction_line_items")

# Daily sales summary
daily_sales = (
    silver_transactions
    .filter(F.col("transaction_type") == "purchase")
    .groupBy("transaction_date", "channel", "store_id")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("transaction_amount").alias("gross_sales"),
        F.sum("total_items").alias("items_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("transaction_amount").alias("avg_transaction_value"),
        F.max("transaction_amount").alias("max_transaction_value"),
        F.sum(F.when(F.col("payment_method") == "credit_card", F.col("transaction_amount")).otherwise(0)).alias("credit_card_sales"),
        F.sum(F.when(F.col("payment_method") == "digital_wallet", F.col("transaction_amount")).otherwise(0)).alias("digital_wallet_sales")
    )
    
    # Calculate returns for net sales
    .alias("sales")
)

# Get returns
daily_returns = (
    silver_transactions
    .filter(F.col("transaction_type") == "return")
    .groupBy("transaction_date", "channel", "store_id")
    .agg(
        F.count("*").alias("return_count"),
        F.sum("transaction_amount").alias("return_amount")
    )
    .alias("returns")
)

# Combine sales and returns
gold_daily_sales = (
    daily_sales
    .join(daily_returns, 
          ["transaction_date", "channel", "store_id"], 
          "left")
    .fillna({"return_count": 0, "return_amount": 0})
    .withColumn("net_sales", F.col("gross_sales") - F.col("return_amount"))
    .withColumn("return_rate", 
        (F.col("return_count") / F.col("transaction_count") * 100).cast("decimal(5,2)"))
    .withColumn("items_per_transaction", 
        (F.col("items_sold") / F.col("transaction_count")).cast("decimal(5,2)"))
    
    # Add time dimensions
    .withColumn("day_of_week", F.dayofweek("transaction_date"))
    .withColumn("week_of_year", F.weekofyear("transaction_date"))
    .withColumn("month", F.month("transaction_date"))
    .withColumn("quarter", F.quarter("transaction_date"))
    .withColumn("year", F.year("transaction_date"))
    .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
    
    .withColumn("gold_processed_at", F.current_timestamp())
)

gold_daily_sales.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['database']}.daily_sales_summary")

print(f"✅ Gold Daily Sales Summary Created: {gold_daily_sales.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: Customer 360

# COMMAND ----------

silver_customers = spark.table(f"{CONFIG['database']}.customers")
silver_transactions = spark.table(f"{CONFIG['database']}.transactions")

# Customer transaction summary
customer_transactions = (
    silver_transactions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.when(F.col("transaction_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.sum(F.when(F.col("transaction_type") == "return", 1).otherwise(0)).alias("return_count"),
        F.sum(F.when(F.col("transaction_type") == "purchase", F.col("transaction_amount")).otherwise(0)).alias("total_spend"),
        F.avg(F.when(F.col("transaction_type") == "purchase", F.col("transaction_amount"))).alias("avg_order_value"),
        F.max(F.when(F.col("transaction_type") == "purchase", F.col("transaction_amount"))).alias("max_order_value"),
        F.sum("total_items").alias("total_items_purchased"),
        F.min("transaction_date").alias("first_purchase_date"),
        F.max("transaction_date").alias("last_purchase_date"),
        F.countDistinct("store_id").alias("stores_visited"),
        F.countDistinct("channel").alias("channels_used"),
        
        # Channel preferences
        F.sum(F.when(F.col("channel") == "online", 1).otherwise(0)).alias("online_orders"),
        F.sum(F.when(F.col("channel") == "in_store", 1).otherwise(0)).alias("instore_orders"),
        F.sum(F.when(F.col("channel") == "mobile_app", 1).otherwise(0)).alias("mobile_orders"),
        
        # Payment preferences
        F.sum(F.when(F.col("payment_method") == "credit_card", 1).otherwise(0)).alias("credit_card_transactions"),
        F.sum(F.when(F.col("payment_method") == "digital_wallet", 1).otherwise(0)).alias("digital_wallet_transactions")
    )
)

# Build Customer 360
customer_360 = (
    silver_customers
    .join(customer_transactions, "customer_id", "left")
    .fillna({
        "total_transactions": 0, "purchase_count": 0, "return_count": 0,
        "total_spend": 0, "total_items_purchased": 0
    })
    
    # Calculate RFM scores (Recency, Frequency, Monetary)
    .withColumn("recency_score",
        F.when(F.col("days_since_purchase") <= 30, 5)
         .when(F.col("days_since_purchase") <= 60, 4)
         .when(F.col("days_since_purchase") <= 90, 3)
         .when(F.col("days_since_purchase") <= 180, 2)
         .otherwise(1))
    .withColumn("frequency_score",
        F.when(F.col("purchase_count") >= 20, 5)
         .when(F.col("purchase_count") >= 10, 4)
         .when(F.col("purchase_count") >= 5, 3)
         .when(F.col("purchase_count") >= 2, 2)
         .otherwise(1))
    .withColumn("monetary_score",
        F.when(F.col("total_spend") >= 5000, 5)
         .when(F.col("total_spend") >= 2000, 4)
         .when(F.col("total_spend") >= 500, 3)
         .when(F.col("total_spend") >= 100, 2)
         .otherwise(1))
    .withColumn("rfm_score", 
        F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"))
    
    # Customer value segment
    .withColumn("value_segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
         .when(F.col("rfm_score") >= 10, "Loyal")
         .when(F.col("rfm_score") >= 7, "Potential")
         .when(F.col("rfm_score") >= 4, "At Risk")
         .otherwise("Lost"))
    
    # Channel preference
    .withColumn("preferred_channel",
        F.when(F.col("online_orders") >= F.greatest("instore_orders", "mobile_orders"), "Online")
         .when(F.col("instore_orders") >= F.greatest("online_orders", "mobile_orders"), "In-Store")
         .when(F.col("mobile_orders") > 0, "Mobile")
         .otherwise("Unknown"))
    
    # Return rate
    .withColumn("return_rate",
        F.when(F.col("purchase_count") > 0, 
               (F.col("return_count") / F.col("purchase_count") * 100).cast("decimal(5,2)"))
         .otherwise(0))
    
    # Predicted lifetime value (simple model)
    .withColumn("predicted_ltv",
        (F.col("avg_order_value") * F.col("frequency_score") * 12).cast("decimal(12,2)"))
    
    .withColumn("gold_processed_at", F.current_timestamp())
)

customer_360.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['database']}.customer_360")

print(f"✅ Gold Customer 360 Created: {customer_360.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: Product Performance

# COMMAND ----------

silver_products = spark.table(f"{CONFIG['database']}.products")
bronze_line_items = spark.table(f"{CONFIG['database']}.transaction_line_items")
silver_transactions = spark.table(f"{CONFIG['database']}.transactions")

# Detailed product performance by time period
product_performance = (
    bronze_line_items
    .join(silver_transactions.select("transaction_id", "transaction_date", "channel", "customer_id"),
          "transaction_id")
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("line_total").alias("total_revenue"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.avg("discount_percent").alias("avg_discount"),
        F.count("*").alias("times_purchased"),
        F.countDistinct("transaction_id").alias("unique_transactions"),
        F.countDistinct("customer_id").alias("unique_customers"),
        
        # Channel breakdown
        F.sum(F.when(F.col("channel") == "online", F.col("line_total")).otherwise(0)).alias("online_revenue"),
        F.sum(F.when(F.col("channel") == "in_store", F.col("line_total")).otherwise(0)).alias("instore_revenue"),
        F.sum(F.when(F.col("channel") == "mobile_app", F.col("line_total")).otherwise(0)).alias("mobile_revenue"),
        
        # Time analysis
        F.min("transaction_date").alias("first_sale_date"),
        F.max("transaction_date").alias("last_sale_date")
    )
)

# Join with product master data
gold_product_performance = (
    silver_products
    .join(product_performance, "product_id", "left")
    .fillna({"total_units_sold": 0, "total_revenue": 0, "unique_customers": 0})
    
    # Calculate metrics
    .withColumn("revenue_per_customer",
        F.when(F.col("unique_customers") > 0, 
               F.col("total_revenue") / F.col("unique_customers"))
         .otherwise(0))
    .withColumn("days_since_first_sale",
        F.datediff(F.current_date(), "first_sale_date"))
    .withColumn("sales_velocity",
        F.when(F.col("days_since_first_sale") > 0,
               F.col("total_units_sold") / F.col("days_since_first_sale"))
         .otherwise(0))
    
    # Product ranking within category
    .withColumn("category_revenue_rank",
        F.row_number().over(
            Window.partitionBy("category").orderBy(F.desc("total_revenue"))))
    .withColumn("category_units_rank",
        F.row_number().over(
            Window.partitionBy("category").orderBy(F.desc("total_units_sold"))))
    
    # Performance classification
    .withColumn("performance_class",
        F.when((F.col("category_revenue_rank") <= 10) & (F.col("sales_velocity") > 1), "Star")
         .when(F.col("category_revenue_rank") <= 50, "Strong")
         .when(F.col("total_units_sold") > 0, "Average")
         .otherwise("Underperformer"))
    
    .withColumn("gold_processed_at", F.current_timestamp())
)

gold_product_performance.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['database']}.product_performance")

print(f"✅ Gold Product Performance Created: {gold_product_performance.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold: Store Analytics

# COMMAND ----------

# Store performance aggregation
bronze_stores = spark.table(f"{CONFIG['database']}.stores")
silver_transactions = spark.table(f"{CONFIG['database']}.transactions")

store_metrics = (
    silver_transactions
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum(F.when(F.col("transaction_type") == "purchase", F.col("transaction_amount")).otherwise(0)).alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.avg("transaction_amount").alias("avg_transaction_value"),
        F.countDistinct("transaction_date").alias("active_days"),
        
        # Time patterns
        F.sum(F.when(F.col("is_weekend"), 1).otherwise(0)).alias("weekend_transactions"),
        F.sum(F.when(~F.col("is_weekend"), 1).otherwise(0)).alias("weekday_transactions"),
        
        # Channel mix (for physical stores selling online too)
        F.sum(F.when(F.col("channel") == "online", 1).otherwise(0)).alias("online_transactions"),
        F.sum(F.when(F.col("channel") == "in_store", 1).otherwise(0)).alias("instore_transactions")
    )
)

gold_store_analytics = (
    bronze_stores
    .join(store_metrics, "store_id", "left")
    .fillna({"total_transactions": 0, "total_revenue": 0, "unique_customers": 0})
    
    # Calculate store metrics
    .withColumn("revenue_per_sqft",
        F.when(F.col("square_footage") > 0, 
               F.col("total_revenue") / F.col("square_footage"))
         .otherwise(0))
    .withColumn("customers_per_sqft",
        F.when(F.col("square_footage") > 0,
               F.col("unique_customers") / F.col("square_footage"))
         .otherwise(0))
    .withColumn("avg_daily_revenue",
        F.when(F.col("active_days") > 0,
               F.col("total_revenue") / F.col("active_days"))
         .otherwise(0))
    .withColumn("weekend_ratio",
        F.when(F.col("total_transactions") > 0,
               F.col("weekend_transactions") / F.col("total_transactions") * 100)
         .otherwise(0))
    
    # Store ranking
    .withColumn("revenue_rank",
        F.row_number().over(Window.orderBy(F.desc("total_revenue"))))
    .withColumn("efficiency_rank",
        F.row_number().over(Window.orderBy(F.desc("revenue_per_sqft"))))
    
    # Performance tier
    .withColumn("performance_tier",
        F.when(F.col("revenue_rank") <= 50, "Top Performer")
         .when(F.col("revenue_rank") <= 200, "Above Average")
         .when(F.col("revenue_rank") <= 350, "Average")
         .otherwise("Below Average"))
    
    .withColumn("gold_processed_at", F.current_timestamp())
)

gold_store_analytics.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['database']}.store_analytics")

print(f"✅ Gold Store Analytics Created: {gold_store_analytics.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE and Z-ORDER Large Tables

# COMMAND ----------

# Optimize gold tables for query performance
optimization_jobs = [
    ("gold.daily_sales_summary", ["transaction_date", "channel"]),
    ("gold.customer_360", ["value_segment", "customer_segment"]),
    ("gold.product_performance", ["category", "performance_class"]),
    ("gold.store_analytics", ["state", "performance_tier"]),
    ("silver.transactions", ["customer_id", "transaction_date"]),
]

for table, zorder_cols in optimization_jobs:
    print(f"Optimizing {table}...")
    spark.sql(f"""
        OPTIMIZE {CONFIG['database']}.{table} 
        ZORDER BY ({', '.join(zorder_cols)})
    """)
    print(f"  ✅ Done")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compute Table Statistics

# COMMAND ----------

# Compute statistics for query optimization
tables = [
    "silver.customers", "silver.transactions", "silver.products",
    "gold.daily_sales_summary", "gold.customer_360", 
    "gold.product_performance", "gold.store_analytics"
]

for table in tables:
    print(f"Computing statistics for {table}...")
    spark.sql(f"ANALYZE TABLE {CONFIG['database']}.{table} COMPUTE STATISTICS FOR ALL COLUMNS")
    print(f"  ✅ Done")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed for Incremental Processing

# COMMAND ----------

# Enable CDF on key tables for downstream incremental processing
cdf_tables = [
    "silver.customers",
    "silver.transactions",
    "gold.customer_360"
]

for table in cdf_tables:
    spark.sql(f"""
        ALTER TABLE {CONFIG['database']}.{table}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"✅ CDF enabled on {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

# Display final data volumes
print("\n" + "=" * 80)
print("BATCH ETL PIPELINE SUMMARY")
print("=" * 80)

layers = {
    "Bronze": ["products", "customers", "stores", "warehouses", "transactions", "transaction_line_items"],
    "Silver": ["customers", "transactions", "products"],
    "Gold": ["daily_sales_summary", "customer_360", "product_performance", "store_analytics"]
}

total_rows = 0
for layer, tables in layers.items():
    print(f"\n{layer} Layer:")
    print("-" * 40)
    for table in tables:
        try:
            count = spark.table(f"{CONFIG['database']}.{layer.lower()}.{table}").count()
            total_rows += count
            print(f"  {table:30} {count:>15,} rows")
        except Exception as e:
            print(f"  {table:30} ERROR: {str(e)[:30]}")

print("\n" + "=" * 80)
print(f"TOTAL ROWS ACROSS ALL LAYERS: {total_rows:,}")
print("=" * 80)
