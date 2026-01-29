# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Synthetic Data Generation
# MAGIC 
# MAGIC This notebook generates high-volume synthetic data for the retail POC:
# MAGIC - **Products**: 100K products across categories
# MAGIC - **Customers**: 10M customers
# MAGIC - **Transactions**: 500M+ historical transactions
# MAGIC - **Clickstream**: Real-time event simulation data
# MAGIC - **Inventory**: Warehouse inventory snapshots
# MAGIC 
# MAGIC **Target Volume**: ~50-100GB raw data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration - adjust based on your needs
CONFIG = {
    "catalog": "sourcefuse_poc",
    "schema_bronze": "bronze",
    "schema_silver": "silver", 
    "schema_gold": "gold",
    "s3_bucket": "s3://databricks-storage-7474650844180840/sourcefuse_poc/",  # Update with your bucket
    "num_products": 100_000,
    "num_customers": 10_000_000,
    "num_transactions": 500_000_000,
    "num_stores": 500,
    "num_warehouses": 50,
    "partitions": 200  # For large data generation
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Unity Catalog (Run once)

# COMMAND ----------

# Create catalog and schemas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CONFIG['catalog']}")
spark.sql(f"USE CATALOG {CONFIG['catalog']}")

for schema in ["bronze", "silver", "gold", "staging"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

print(f"✅ Created catalog '{CONFIG['catalog']}' with bronze/silver/gold/staging schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Reference Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta

# Product categories with realistic hierarchy
CATEGORIES = {
    "Electronics": ["Smartphones", "Laptops", "Tablets", "Headphones", "Cameras", "Smart Home", "Gaming", "Wearables"],
    "Clothing": ["Men's Apparel", "Women's Apparel", "Kids", "Shoes", "Accessories", "Sportswear", "Outerwear"],
    "Home & Garden": ["Furniture", "Kitchen", "Bedding", "Decor", "Garden", "Storage", "Lighting", "Appliances"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Winter Sports", "Cycling", "Running"],
    "Beauty": ["Skincare", "Makeup", "Haircare", "Fragrance", "Personal Care", "Men's Grooming"],
    "Food & Beverage": ["Snacks", "Beverages", "Grocery", "Organic", "Specialty", "International"],
    "Toys & Games": ["Action Figures", "Board Games", "Puzzles", "Educational", "Outdoor Play", "Video Games"],
    "Books & Media": ["Fiction", "Non-Fiction", "Children's", "Audiobooks", "Music", "Movies"]
}

# US States for geographic distribution
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Products (100K)

# COMMAND ----------

from pyspark.sql import Row

def generate_products(num_products):
    """Generate product dimension data"""
    products = []
    product_id = 1
    
    for category, subcategories in CATEGORIES.items():
        products_per_subcategory = num_products // (len(CATEGORIES) * len(subcategories))
        
        for subcategory in subcategories:
            for i in range(products_per_subcategory):
                base_price = random.uniform(5.99, 999.99)
                products.append(Row(
                    product_id=product_id,
                    product_sku=f"SKU-{category[:3].upper()}-{product_id:08d}",
                    product_name=f"{subcategory} Item {i+1}",
                    category=category,
                    subcategory=subcategory,
                    brand=f"Brand_{random.randint(1, 500)}",
                    base_price=round(base_price, 2),
                    cost_price=round(base_price * random.uniform(0.3, 0.7), 2),
                    weight_kg=round(random.uniform(0.1, 50.0), 2),
                    is_active=random.random() > 0.05,
                    created_date=datetime.now() - timedelta(days=random.randint(1, 1825)),
                    updated_date=datetime.now()
                ))
                product_id += 1
    
    return products

# Generate and save products
products_data = generate_products(CONFIG["num_products"])
products_df = spark.createDataFrame(products_data)

products_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['catalog']}.bronze.products")

print(f"✅ Generated {products_df.count():,} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Customers (10M) - Distributed

# COMMAND ----------

# Use Spark's native data generation for scale
from pyspark.sql.functions import (
    expr, rand, floor, concat, lit, array, element_at,
    date_sub, current_date, to_date, sha2, col
)

# Create states array for random selection
states_expr = f"array({', '.join([repr(s) for s in US_STATES])})"

customers_df = (
    spark.range(1, CONFIG["num_customers"] + 1)
    .withColumn("customer_id", col("id"))
    .withColumn("email", concat(lit("customer_"), col("id"), lit("@example.com")))
    .withColumn("first_name", concat(lit("First_"), (col("id") % 10000).cast("string")))
    .withColumn("last_name", concat(lit("Last_"), (col("id") % 50000).cast("string")))
    .withColumn("state", element_at(expr(states_expr), (floor(rand() * 50) + 1).cast("int")))
    .withColumn("city", concat(lit("City_"), (floor(rand() * 1000) + 1).cast("string")))
    .withColumn("zip_code", (floor(rand() * 90000) + 10000).cast("string"))
    .withColumn("phone", concat(lit("+1"), (floor(rand() * 9000000000) + 1000000000).cast("string")))
    .withColumn("customer_segment", 
        expr("CASE WHEN rand() < 0.05 THEN 'VIP' " +
             "WHEN rand() < 0.20 THEN 'Premium' " +
             "WHEN rand() < 0.50 THEN 'Regular' " +
             "ELSE 'New' END"))
    .withColumn("lifetime_value", (rand() * 10000).cast("decimal(10,2)"))
    .withColumn("registration_date", date_sub(current_date(), (rand() * 1825).cast("int")))
    .withColumn("last_purchase_date", date_sub(current_date(), (rand() * 365).cast("int")))
    .withColumn("is_active", rand() > 0.1)
    .drop("id")
    .repartition(CONFIG["partitions"])
)

customers_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['catalog']}.bronze.customers")

print(f"✅ Generated {customers_df.count():,} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Stores & Warehouses

# COMMAND ----------

# Generate stores
stores_df = (
    spark.range(1, CONFIG["num_stores"] + 1)
    .withColumn("store_id", col("id"))
    .withColumn("store_name", concat(lit("Store #"), col("id")))
    .withColumn("store_type", expr("CASE WHEN rand() < 0.3 THEN 'Flagship' WHEN rand() < 0.6 THEN 'Standard' ELSE 'Outlet' END"))
    .withColumn("state", element_at(expr(states_expr), (floor(rand() * 50) + 1).cast("int")))
    .withColumn("city", concat(lit("City_"), (floor(rand() * 200) + 1).cast("string")))
    .withColumn("square_footage", (floor(rand() * 50000) + 5000).cast("int"))
    .withColumn("open_date", date_sub(current_date(), (rand() * 3650).cast("int")))
    .withColumn("is_active", rand() > 0.05)
    .drop("id")
)

stores_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CONFIG['catalog']}.bronze.stores")

# Generate warehouses
warehouses_df = (
    spark.range(1, CONFIG["num_warehouses"] + 1)
    .withColumn("warehouse_id", col("id"))
    .withColumn("warehouse_name", concat(lit("Warehouse #"), col("id")))
    .withColumn("warehouse_type", expr("CASE WHEN rand() < 0.4 THEN 'Distribution Center' WHEN rand() < 0.7 THEN 'Fulfillment Center' ELSE 'Regional Hub' END"))
    .withColumn("state", element_at(expr(states_expr), (floor(rand() * 50) + 1).cast("int")))
    .withColumn("capacity_units", (floor(rand() * 1000000) + 100000).cast("int"))
    .withColumn("is_active", lit(True))
    .drop("id")
)

warehouses_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CONFIG['catalog']}.bronze.warehouses")

print(f"✅ Generated {CONFIG['num_stores']} stores and {CONFIG['num_warehouses']} warehouses")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Transactions (500M) - Highly Parallelized

# COMMAND ----------

# MAGIC %md
# MAGIC This is the heavy workload - generating 500M transactions with realistic patterns

# COMMAND ----------

from pyspark.sql.functions import explode, sequence, monotonically_increasing_id

# Configuration for transaction generation
TRANSACTIONS_PER_BATCH = 50_000_000  # 50M per batch
NUM_BATCHES = CONFIG["num_transactions"] // TRANSACTIONS_PER_BATCH

print(f"Generating {CONFIG['num_transactions']:,} transactions in {NUM_BATCHES} batches...")

# COMMAND ----------

def generate_transaction_batch(batch_num, batch_size, num_customers, num_products, num_stores):
    """Generate a batch of transactions using pure Spark operations"""
    
    start_id = batch_num * batch_size + 1
    
    transactions_df = (
        spark.range(start_id, start_id + batch_size)
        .withColumn("transaction_id", col("id"))
        .withColumn("customer_id", (floor(rand() * num_customers) + 1).cast("long"))
        .withColumn("store_id", (floor(rand() * num_stores) + 1).cast("int"))
        # Weighted towards recent transactions (exponential decay)
        .withColumn("days_ago", (F.pow(rand(), 0.3) * 730).cast("int"))  # Last 2 years, weighted recent
        .withColumn("transaction_date", date_sub(current_date(), col("days_ago")))
        .withColumn("transaction_timestamp", 
            (F.unix_timestamp(col("transaction_date")) + (rand() * 86400)).cast("timestamp"))
        .withColumn("transaction_type", 
            expr("CASE WHEN rand() < 0.85 THEN 'purchase' WHEN rand() < 0.95 THEN 'return' ELSE 'exchange' END"))
        .withColumn("payment_method",
            expr("CASE WHEN rand() < 0.45 THEN 'credit_card' WHEN rand() < 0.70 THEN 'debit_card' " +
                 "WHEN rand() < 0.85 THEN 'digital_wallet' WHEN rand() < 0.95 THEN 'cash' ELSE 'gift_card' END"))
        .withColumn("channel",
            expr("CASE WHEN rand() < 0.65 THEN 'online' WHEN rand() < 0.90 THEN 'in_store' ELSE 'mobile_app' END"))
        # Number of items per transaction (1-10, weighted towards lower)
        .withColumn("num_items", (floor(F.pow(rand(), 2) * 9) + 1).cast("int"))
        .drop("id", "days_ago")
    )
    
    return transactions_df

# Generate all batches and write as partitioned Delta table
for batch in range(NUM_BATCHES):
    print(f"Processing batch {batch + 1}/{NUM_BATCHES}...")
    
    batch_df = generate_transaction_batch(
        batch, 
        TRANSACTIONS_PER_BATCH, 
        CONFIG["num_customers"], 
        CONFIG["num_products"],
        CONFIG["num_stores"]
    )
    
    # Partition by year-month for efficient querying
    batch_df = batch_df.withColumn("year_month", F.date_format("transaction_date", "yyyy-MM"))
    
    write_mode = "overwrite" if batch == 0 else "append"
    
    batch_df.repartition(CONFIG["partitions"], "year_month") \
        .write.format("delta") \
        .mode(write_mode) \
        .partitionBy("year_month") \
        .option("overwriteSchema", "true" if batch == 0 else "false") \
        .saveAsTable(f"{CONFIG['catalog']}.bronze.transactions")
    
    print(f"  ✅ Batch {batch + 1} complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Transaction Line Items

# COMMAND ----------

# Read transactions and explode to line items
transactions_df = spark.table(f"{CONFIG['catalog']}.bronze.transactions")

line_items_df = (
    transactions_df
    .withColumn("item_seq", explode(sequence(lit(1), col("num_items"))))
    .withColumn("line_item_id", monotonically_increasing_id())
    .withColumn("product_id", (floor(rand() * CONFIG["num_products"]) + 1).cast("long"))
    .withColumn("quantity", (floor(F.pow(rand(), 2) * 4) + 1).cast("int"))  # 1-5, weighted low
    .withColumn("unit_price", (rand() * 500 + 5).cast("decimal(10,2)"))
    .withColumn("discount_percent", 
        expr("CASE WHEN rand() < 0.7 THEN 0 ELSE floor(rand() * 30) + 5 END").cast("decimal(5,2)"))
    .withColumn("line_total", 
        (col("quantity") * col("unit_price") * (1 - col("discount_percent") / 100)).cast("decimal(12,2)"))
    .select(
        "line_item_id", "transaction_id", "product_id", "item_seq",
        "quantity", "unit_price", "discount_percent", "line_total",
        "transaction_date", "year_month"
    )
    .repartition(CONFIG["partitions"], "year_month")
)

line_items_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year_month") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['catalog']}.bronze.transaction_line_items")

print(f"✅ Generated transaction line items")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Inventory Snapshots

# COMMAND ----------

# Cross join products and warehouses for inventory
products_df = spark.table(f"{CONFIG['catalog']}.bronze.products").select("product_id")
warehouses_df = spark.table(f"{CONFIG['catalog']}.bronze.warehouses").select("warehouse_id")

# Generate daily inventory snapshots for last 30 days
dates_df = spark.range(0, 30).withColumn("snapshot_date", date_sub(current_date(), col("id").cast("int")))

inventory_df = (
    products_df.crossJoin(warehouses_df).crossJoin(dates_df.select("snapshot_date"))
    .withColumn("inventory_id", monotonically_increasing_id())
    .withColumn("quantity_on_hand", (floor(rand() * 1000)).cast("int"))
    .withColumn("quantity_reserved", (floor(rand() * col("quantity_on_hand") * 0.3)).cast("int"))
    .withColumn("quantity_available", col("quantity_on_hand") - col("quantity_reserved"))
    .withColumn("reorder_point", (floor(rand() * 100) + 20).cast("int"))
    .withColumn("reorder_quantity", (floor(rand() * 500) + 100).cast("int"))
    .withColumn("last_restock_date", date_sub(col("snapshot_date"), (rand() * 30).cast("int")))
    .withColumn("unit_cost", (rand() * 200 + 5).cast("decimal(10,2)"))
    .repartition(CONFIG["partitions"], "snapshot_date")
)

inventory_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("snapshot_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['catalog']}.bronze.inventory_snapshots")

print(f"✅ Generated inventory snapshots")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Clickstream Events (for Streaming Simulation)

# COMMAND ----------

# Create a clickstream events template table for streaming simulation
event_types = ["page_view", "product_view", "add_to_cart", "remove_from_cart", 
               "checkout_start", "checkout_complete", "search", "filter_apply", "wishlist_add"]

clickstream_sample_df = (
    spark.range(1, 1_000_001)  # 1M sample events
    .withColumn("event_id", concat(lit("evt_"), sha2(col("id").cast("string"), 256).substr(1, 16)))
    .withColumn("session_id", concat(lit("sess_"), (floor(col("id") / 10) % 100000).cast("string")))
    .withColumn("customer_id", 
        expr(f"CASE WHEN rand() < 0.3 THEN NULL ELSE floor(rand() * {CONFIG['num_customers']}) + 1 END").cast("long"))
    .withColumn("event_type", 
        element_at(expr(f"array({', '.join([repr(e) for e in event_types])})"), (floor(rand() * 9) + 1).cast("int")))
    .withColumn("product_id",
        expr(f"CASE WHEN rand() < 0.4 THEN NULL ELSE floor(rand() * {CONFIG['num_products']}) + 1 END").cast("long"))
    .withColumn("page_url", concat(lit("/page/"), (floor(rand() * 1000) + 1).cast("string")))
    .withColumn("referrer", 
        expr("CASE WHEN rand() < 0.3 THEN 'google' WHEN rand() < 0.5 THEN 'facebook' " +
             "WHEN rand() < 0.7 THEN 'direct' WHEN rand() < 0.85 THEN 'email' ELSE 'other' END"))
    .withColumn("device_type",
        expr("CASE WHEN rand() < 0.55 THEN 'mobile' WHEN rand() < 0.85 THEN 'desktop' ELSE 'tablet' END"))
    .withColumn("browser",
        expr("CASE WHEN rand() < 0.4 THEN 'chrome' WHEN rand() < 0.65 THEN 'safari' " +
             "WHEN rand() < 0.8 THEN 'firefox' WHEN rand() < 0.9 THEN 'edge' ELSE 'other' END"))
    .withColumn("event_timestamp", 
        (F.unix_timestamp(current_date()) - rand() * 86400 * 7).cast("timestamp"))
    .withColumn("event_properties", 
        F.to_json(F.struct(
            (rand() * 100).cast("int").alias("scroll_depth"),
            (rand() * 300).cast("int").alias("time_on_page_seconds")
        )))
    .drop("id")
)

clickstream_sample_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CONFIG['catalog']}.bronze.clickstream_sample")

print(f"✅ Generated clickstream sample data for streaming simulation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Summary

# COMMAND ----------

# Display summary of generated data
tables = ["products", "customers", "stores", "warehouses", "transactions", 
          "transaction_line_items", "inventory_snapshots", "clickstream_sample"]

print("=" * 60)
print("DATA GENERATION SUMMARY")
print("=" * 60)

total_rows = 0
for table in tables:
    try:
        count = spark.table(f"{CONFIG['catalog']}.bronze.{table}").count()
        total_rows += count
        print(f"{table:30} {count:>15,} rows")
    except Exception as e:
        print(f"{table:30} ERROR: {str(e)}")

print("=" * 60)
print(f"{'TOTAL':30} {total_rows:>15,} rows")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Tables for Performance

# COMMAND ----------

# Optimize and Z-Order the large tables for query performance
optimization_configs = [
    ("transactions", ["customer_id", "store_id"]),
    ("transaction_line_items", ["product_id", "transaction_id"]),
    ("inventory_snapshots", ["product_id", "warehouse_id"]),
    ("customers", ["state", "customer_segment"]),
]

for table, zorder_cols in optimization_configs:
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {CONFIG['catalog']}.bronze.{table} ZORDER BY ({', '.join(zorder_cols)})")
    print(f"  ✅ {table} optimized")

# Compute statistics for query planning
for table in tables:
    print(f"Computing stats for {table}...")
    spark.sql(f"ANALYZE TABLE {CONFIG['catalog']}.bronze.{table} COMPUTE STATISTICS FOR ALL COLUMNS")

print("\n✅ All tables optimized and statistics computed")
