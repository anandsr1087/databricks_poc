# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Streaming Pipeline
# MAGIC 
# MAGIC This notebook demonstrates high-throughput streaming ingestion using:
# MAGIC - **Structured Streaming** with Delta Lake
# MAGIC - **Auto Loader** for incremental file ingestion
# MAGIC - **Trigger.AvailableNow** for efficient micro-batch processing
# MAGIC - **Streaming aggregations** with watermarking
# MAGIC 
# MAGIC **Target**: 10K-100K events/second throughput

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CONFIG = {
    "catalog": "sourcefuse_poc",
    "checkpoint_base": "/tmp/checkpoints/sourcefuse_poc",
    "s3_landing_zone": "s3://databricks-storage-7474650844180840/sourcefuse_poc/landing",  # Update with your bucket
    "events_per_second": 50000,  # Target throughput
    "batch_interval": "10 seconds"
}

spark.sql(f"USE CATALOG {CONFIG['catalog']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Clickstream Event Streaming
# MAGIC 
# MAGIC Simulates real-time clickstream ingestion with:
# MAGIC - High-throughput event processing
# MAGIC - Schema enforcement
# MAGIC - Exactly-once semantics via Delta Lake

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

# Define clickstream schema
clickstream_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("customer_id", LongType(), True),
    StructField("event_type", StringType(), False),
    StructField("product_id", LongType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("event_timestamp", TimestampType(), False),
    StructField("event_properties", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Rate Source (For POC Demonstration)
# MAGIC 
# MAGIC Use Spark's built-in rate source to simulate high-throughput streaming

# COMMAND ----------

from pyspark.sql.functions import (
    expr, rand, floor, concat, lit, sha2, col, 
    current_timestamp, to_json, struct, element_at, array
)

# Event types for random selection
event_types_expr = "array('page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 'checkout_start', 'checkout_complete', 'search', 'filter_apply', 'wishlist_add')"

# Create high-throughput stream using rate source
rate_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", CONFIG["events_per_second"])
    .option("numPartitions", 32)  # Parallelize generation
    .load()
)

# Transform rate stream to clickstream events
clickstream_stream = (
    rate_stream
    .withColumn("event_id", concat(lit("evt_"), sha2(col("value").cast("string"), 256).substr(1, 16)))
    .withColumn("session_id", concat(lit("sess_"), (floor(rand() * 100000)).cast("string")))
    .withColumn("customer_id", 
        expr("CASE WHEN rand() < 0.3 THEN NULL ELSE floor(rand() * 10000000) + 1 END").cast("long"))
    .withColumn("event_type", 
        element_at(expr(event_types_expr), (floor(rand() * 9) + 1).cast("int")))
    .withColumn("product_id",
        expr("CASE WHEN rand() < 0.4 THEN NULL ELSE floor(rand() * 100000) + 1 END").cast("long"))
    .withColumn("page_url", concat(lit("/page/"), (floor(rand() * 1000) + 1).cast("string")))
    .withColumn("referrer", 
        expr("CASE WHEN rand() < 0.3 THEN 'google' WHEN rand() < 0.5 THEN 'facebook' " +
             "WHEN rand() < 0.7 THEN 'direct' WHEN rand() < 0.85 THEN 'email' ELSE 'other' END"))
    .withColumn("device_type",
        expr("CASE WHEN rand() < 0.55 THEN 'mobile' WHEN rand() < 0.85 THEN 'desktop' ELSE 'tablet' END"))
    .withColumn("browser",
        expr("CASE WHEN rand() < 0.4 THEN 'chrome' WHEN rand() < 0.65 THEN 'safari' " +
             "WHEN rand() < 0.8 THEN 'firefox' WHEN rand() < 0.9 THEN 'edge' ELSE 'other' END"))
    .withColumn("event_timestamp", col("timestamp"))
    .withColumn("event_properties", 
        to_json(struct(
            (rand() * 100).cast("int").alias("scroll_depth"),
            (rand() * 300).cast("int").alias("time_on_page_seconds")
        )))
    .withColumn("event_date", F.to_date("event_timestamp"))
    .withColumn("event_hour", F.hour("event_timestamp"))
    .select(
        "event_id", "session_id", "customer_id", "event_type", "product_id",
        "page_url", "referrer", "device_type", "browser", "event_timestamp",
        "event_properties", "event_date", "event_hour"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Streaming Data to Delta Lake (Bronze)

# COMMAND ----------

# Write clickstream to bronze Delta table
clickstream_query = (
    clickstream_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CONFIG['checkpoint_base']}/clickstream_bronze")
    .option("mergeSchema", "true")
    .partitionBy("event_date", "event_hour")
    .trigger(processingTime=CONFIG["batch_interval"])
    .toTable(f"{CONFIG['catalog']}.bronze.clickstream_events")
)

print(f"✅ Clickstream bronze ingestion started")
print(f"   Target: {CONFIG['events_per_second']:,} events/second")
print(f"   Table: {CONFIG['catalog']}.bronze.clickstream_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor Stream Performance

# COMMAND ----------

import time

def monitor_stream(query, duration_seconds=60, interval_seconds=10):
    """Monitor streaming query performance"""
    print(f"\nMonitoring stream for {duration_seconds} seconds...")
    print("-" * 80)
    
    start_time = time.time()
    total_rows = 0
    
    while time.time() - start_time < duration_seconds:
        if not query.isActive:
            print("Stream stopped!")
            break
            
        progress = query.lastProgress
        if progress:
            input_rows = progress.get('numInputRows', 0)
            processing_rate = progress.get('inputRowsPerSecond', 0)
            process_rate = progress.get('processedRowsPerSecond', 0)
            
            total_rows += input_rows
            
            print(f"[{time.strftime('%H:%M:%S')}] "
                  f"Input: {input_rows:>8,} rows | "
                  f"Rate In: {processing_rate:>10,.0f}/s | "
                  f"Rate Process: {process_rate:>10,.0f}/s | "
                  f"Total: {total_rows:>12,}")
        
        time.sleep(interval_seconds)
    
    print("-" * 80)
    print(f"Total rows processed: {total_rows:,}")
    return total_rows

# Monitor the stream (run for 60 seconds)
# Uncomment to run: monitor_stream(clickstream_query, duration_seconds=60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Streaming Aggregations (Silver Layer)
# MAGIC 
# MAGIC Real-time metrics with windowed aggregations and watermarking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real-time Session Analytics

# COMMAND ----------

from pyspark.sql.functions import window, count, countDistinct, sum as spark_sum, avg

# Read from bronze clickstream as stream
clickstream_bronze = (
    spark.readStream
    .format("delta")
    .table(f"{CONFIG['catalog']}.bronze.clickstream_events")
)

# Windowed session aggregations with watermarking
session_metrics = (
    clickstream_bronze
    .withWatermark("event_timestamp", "1 hour")  # Allow 1 hour late data
    .groupBy(
        window("event_timestamp", "5 minutes", "1 minute"),  # 5-min windows, 1-min slide
        "device_type",
        "referrer"
    )
    .agg(
        count("*").alias("event_count"),
        countDistinct("session_id").alias("unique_sessions"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_sum(F.when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
        spark_sum(F.when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_carts"),
        spark_sum(F.when(col("event_type") == "checkout_complete", 1).otherwise(0)).alias("checkouts")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .withColumn("conversion_rate", 
        (col("checkouts") / col("unique_sessions") * 100).cast("decimal(5,2)"))
    .drop("window")
)

# Write to silver layer
session_metrics_query = (
    session_metrics
    .writeStream
    .format("delta")
    .outputMode("update")  # Update mode for aggregations
    .option("checkpointLocation", f"{CONFIG['checkpoint_base']}/session_metrics_silver")
    .trigger(processingTime="30 seconds")
    .toTable(f"{CONFIG['catalog']}.silver.session_metrics")
)

print("✅ Session metrics streaming aggregation started")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real-time Product Trending

# COMMAND ----------

# Product trending - which products are hot right now
product_trending = (
    clickstream_bronze
    .filter(col("product_id").isNotNull())
    .withWatermark("event_timestamp", "30 minutes")
    .groupBy(
        window("event_timestamp", "15 minutes", "5 minutes"),
        "product_id"
    )
    .agg(
        count("*").alias("total_events"),
        countDistinct("session_id").alias("unique_viewers"),
        spark_sum(F.when(col("event_type") == "product_view", 1).otherwise(0)).alias("views"),
        spark_sum(F.when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
        spark_sum(F.when(col("event_type") == "checkout_complete", 1).otherwise(0)).alias("purchases")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .withColumn("engagement_score", 
        (col("views") + col("cart_adds") * 3 + col("purchases") * 10).cast("int"))
    .drop("window")
)

product_trending_query = (
    product_trending
    .writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CONFIG['checkpoint_base']}/product_trending_silver")
    .trigger(processingTime="30 seconds")
    .toTable(f"{CONFIG['catalog']}.silver.product_trending")
)

print("✅ Product trending streaming started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Auto Loader for File-based Streaming
# MAGIC 
# MAGIC Demonstrates incremental file ingestion from S3/ADLS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto Loader Configuration

# COMMAND ----------

# Auto Loader for JSON files landing in S3
# This simulates real-world file-based data ingestion

autoloader_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", LongType(), False),
    StructField("order_date", StringType(), False),
    StructField("items", ArrayType(StructType([
        StructField("product_id", LongType()),
        StructField("quantity", IntegerType()),
        StructField("price", DoubleType())
    ]))),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
    StructField("shipping_address", StructType([
        StructField("street", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("zip", StringType())
    ]))
])

# Note: Update the path to your S3 bucket
# autoloader_stream = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.schemaLocation", f"{CONFIG['checkpoint_base']}/autoloader_schema")
#     .option("cloudFiles.inferColumnTypes", "true")
#     .option("cloudFiles.maxFilesPerTrigger", 1000)
#     .option("cloudFiles.useNotifications", "true")  # Use S3 notifications for efficiency
#     .schema(autoloader_schema)
#     .load(f"{CONFIG['s3_landing_zone']}/orders/")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Stream-to-Stream Joins

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Clickstream with Customer Data

# COMMAND ----------

# Read customers as static (for stream-static join)
customers_df = spark.table(f"{CONFIG['catalog']}.bronze.customers").select(
    "customer_id", "customer_segment", "state", "lifetime_value"
)

# Enrich clickstream with customer data (stream-static join)
enriched_clickstream = (
    clickstream_bronze
    .join(customers_df, "customer_id", "left")
    .select(
        clickstream_bronze["*"],
        customers_df["customer_segment"],
        customers_df["state"].alias("customer_state"),
        customers_df["lifetime_value"]
    )
)

enriched_query = (
    enriched_clickstream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CONFIG['checkpoint_base']}/enriched_clickstream")
    .partitionBy("event_date")
    .trigger(processingTime="30 seconds")
    .toTable(f"{CONFIG['catalog']}.silver.clickstream_enriched")
)

print("✅ Enriched clickstream stream started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Streaming Dashboard Metrics

# COMMAND ----------

# Create a streaming dashboard view updated every 10 seconds
dashboard_metrics = (
    clickstream_bronze
    .withWatermark("event_timestamp", "5 minutes")
    .groupBy(
        window("event_timestamp", "1 minute")
    )
    .agg(
        count("*").alias("events_per_minute"),
        countDistinct("session_id").alias("active_sessions"),
        countDistinct("customer_id").alias("active_customers"),
        spark_sum(F.when(col("event_type") == "checkout_complete", 1).otherwise(0)).alias("conversions")
    )
    .withColumn("timestamp", col("window.end"))
    .drop("window")
)

# Write to a metrics table for real-time dashboards
dashboard_query = (
    dashboard_metrics
    .writeStream
    .format("delta")
    .outputMode("update")
    .option("checkpointLocation", f"{CONFIG['checkpoint_base']}/dashboard_metrics")
    .trigger(processingTime="10 seconds")
    .toTable(f"{CONFIG['catalog']}.gold.realtime_dashboard")
)

print("✅ Real-time dashboard metrics stream started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Management

# COMMAND ----------

# List all active streams
def list_active_streams():
    """Display all active streaming queries"""
    print("\n" + "=" * 80)
    print("ACTIVE STREAMING QUERIES")
    print("=" * 80)
    
    for stream in spark.streams.active:
        status = stream.status
        print(f"\nQuery: {stream.name or stream.id}")
        print(f"  Status: {'Running' if stream.isActive else 'Stopped'}")
        print(f"  Data Available: {status.get('isDataAvailable', 'N/A')}")
        print(f"  Trigger Active: {status.get('isTriggerActive', 'N/A')}")
        
        if stream.lastProgress:
            progress = stream.lastProgress
            print(f"  Input Rows/sec: {progress.get('inputRowsPerSecond', 0):,.0f}")
            print(f"  Processed Rows/sec: {progress.get('processedRowsPerSecond', 0):,.0f}")
            print(f"  Batch ID: {progress.get('batchId', 'N/A')}")

list_active_streams()

# COMMAND ----------

# Stop all streams (run when done with POC)
def stop_all_streams():
    """Stop all active streaming queries"""
    for stream in spark.streams.active:
        print(f"Stopping {stream.name or stream.id}...")
        stream.stop()
    print("\n✅ All streams stopped")

# Uncomment to stop: stop_all_streams()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Performance Validation

# COMMAND ----------

# Validate streaming data volumes
tables_to_check = [
    ("bronze.clickstream_events", "event_date"),
    ("silver.session_metrics", "window_start"),
    ("silver.product_trending", "window_start"),
    ("silver.clickstream_enriched", "event_date"),
    ("gold.realtime_dashboard", "timestamp")
]

print("\n" + "=" * 80)
print("STREAMING DATA VOLUMES")
print("=" * 80)

for table, date_col in tables_to_check:
    try:
        df = spark.table(f"{CONFIG['catalog']}.{table}")
        count = df.count()
        
        # Get latest timestamp
        latest = df.agg(F.max(date_col)).collect()[0][0]
        
        print(f"\n{table}")
        print(f"  Total Rows: {count:,}")
        print(f"  Latest: {latest}")
    except Exception as e:
        print(f"\n{table}: Not yet created or error - {str(e)[:50]}")
