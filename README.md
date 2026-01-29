# Databricks Workload-Heavy POC

## Comprehensive Guide for Performance & Scalability Demonstration

---

## Executive Summary

This Proof of Concept (POC) demonstrates Databricks' capabilities for handling heavy workloads across batch ETL, real-time streaming, and SQL analytics. The POC showcases enterprise features including Delta Lake, Unity Catalog, Photon acceleration, and Databricks Workflows.

### Key Metrics Targeted

| Workload Type | Target | Features Demonstrated |
|---------------|--------|----------------------|
| Batch ETL | 500M+ transactions processed | Delta Lake, Medallion Architecture, OPTIMIZE/Z-ORDER |
| Streaming | 50K-100K events/second | Structured Streaming, Auto Loader, Watermarking |
| SQL Analytics | Sub-second queries on 1B+ rows | Photon, Materialized Views, Concurrency |
| Governance | Full lineage & access control | Unity Catalog, Tags, Row/Column Security |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATABRICKS RETAIL POC                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐       │
│  │   DATA SOURCES  │     │   STREAMING     │     │   BATCH FILES   │       │
│  │                 │     │   (Kafka/Rate)  │     │   (S3/ADLS)     │       │
│  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘       │
│           │                       │                       │                 │
│           ▼                       ▼                       ▼                 │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                        BRONZE LAYER                              │       │
│  │  • Raw data ingestion          • Schema evolution               │       │
│  │  • Auto Loader                 • Data validation                │       │
│  │  Tables: transactions, customers, products, clickstream         │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│                                   │                                         │
│                                   ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                        SILVER LAYER                              │       │
│  │  • Data cleansing             • Standardization                 │       │
│  │  • Deduplication              • Data quality flags              │       │
│  │  • Enrichment                 • Business rules                  │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│                                   │                                         │
│                                   ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                         GOLD LAYER                               │       │
│  │  • Business aggregations      • Customer 360                    │       │
│  │  • KPI calculations           • Product performance             │       │
│  │  • Materialized views         • Real-time dashboards            │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│                                   │                                         │
│           ┌───────────────────────┼───────────────────────┐                │
│           ▼                       ▼                       ▼                │
│  ┌─────────────┐         ┌─────────────┐         ┌─────────────┐          │
│  │ SQL         │         │ BI Tools    │         │ ML/AI       │          │
│  │ Warehouse   │         │ (Tableau)   │         │ Workloads   │          │
│  └─────────────┘         └─────────────┘         └─────────────┘          │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────┐       │
│  │                     UNITY CATALOG                                │       │
│  │  • Access Control  • Data Lineage  • Tags  • Audit Logs        │       │
│  └─────────────────────────────────────────────────────────────────┘       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Model

### Synthetic Data Generation

The POC generates realistic retail data at scale:

| Table | Record Count | Description |
|-------|-------------|-------------|
| products | 100,000 | Product catalog with 8 categories, 64 subcategories |
| customers | 10,000,000 | Customer master with segments and demographics |
| stores | 500 | Physical retail locations |
| sourcefuse_pocs | 50 | Distribution and fulfillment centers |
| transactions | 500,000,000 | 2 years of transaction history |
| transaction_line_items | 1,500,000,000+ | Individual line items per transaction |
| inventory_snapshots | 150,000,000 | Daily inventory positions |
| clickstream_events | Streaming | Real-time user behavior events |

### Entity Relationship

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│   PRODUCTS   │       │  CUSTOMERS   │       │    STORES    │
├──────────────┤       ├──────────────┤       ├──────────────┤
│ product_id   │       │ customer_id  │       │ store_id     │
│ product_sku  │       │ email        │       │ store_name   │
│ category     │       │ first_name   │       │ store_type   │
│ subcategory  │       │ last_name    │       │ state        │
│ base_price   │       │ state        │       │ square_ft    │
│ cost_price   │       │ segment      │       │ open_date    │
└──────┬───────┘       └──────┬───────┘       └──────┬───────┘
       │                      │                      │
       │         ┌────────────┴────────────┐        │
       │         │                         │        │
       │         ▼                         ▼        │
       │  ┌──────────────┐         ┌──────────────┐│
       │  │ TRANSACTIONS │         │ CLICKSTREAM  ││
       │  ├──────────────┤         ├──────────────┤│
       │  │ transaction_id│        │ event_id     ││
       └──│ customer_id  │         │ session_id   │┘
          │ store_id ────┼─────────│ customer_id  │
          │ trans_date   │         │ event_type   │
          │ channel      │         │ product_id ──┼──┐
          │ amount       │         │ timestamp    │  │
          └──────┬───────┘         └──────────────┘  │
                 │                                   │
                 ▼                                   │
          ┌──────────────┐                          │
          │ LINE_ITEMS   │                          │
          ├──────────────┤                          │
          │ line_item_id │                          │
          │ transaction_id                          │
          │ product_id ──┼──────────────────────────┘
          │ quantity     │
          │ unit_price   │
          │ discount     │
          └──────────────┘
```

---

## Cluster Configuration Recommendations

### ETL Cluster (Batch Processing)

```
Instance Type: r5d.4xlarge (AWS) / Standard_E16ds_v4 (Azure)
Workers: 8-16
Features:
  - Memory optimized for large shuffles
  - Local NVMe SSD for spill
  - Photon enabled
  - Auto-scaling: 4-16 workers

Spark Configuration:
  spark.databricks.delta.optimizeWrite.enabled: true
  spark.databricks.delta.autoCompact.enabled: true
  spark.sql.shuffle.partitions: 200
  spark.databricks.photon.enabled: true
```

### Streaming Cluster

```
Instance Type: r5d.2xlarge (AWS) / Standard_E8ds_v4 (Azure)
Workers: 4 (min) - 8 (max)
Features:
  - Auto-scaling enabled
  - ON_DEMAND for stability
  - Photon for aggregations

Spark Configuration:
  spark.sql.streaming.schemaInference: true
  spark.databricks.delta.optimizeWrite.enabled: true
```

### SQL Warehouse

```
Size: Large (or 2X-Large for concurrency testing)
Type: Pro (Photon-accelerated)
Features:
  - Auto-stop: 10 minutes
  - Scaling: 1-4 clusters
  - Spot instances: 80%
```

---

## Notebook Inventory

| Notebook | Purpose | Key Features |
|----------|---------|--------------|
| 01_data_generation.py | Generate synthetic data | Distributed data generation, Delta tables |
| 02_streaming_pipeline.py | Real-time ingestion | Structured Streaming, watermarks, aggregations |
| 03_batch_etl_pipeline.py | Medallion architecture | Bronze→Silver→Gold, MERGE, data quality |
| 04_sql_analytics.py | SQL performance testing | Photon, benchmarks, concurrency |
| 05_unity_catalog_governance.py | Governance setup | Access control, lineage, tagging |
| 06_workflows_orchestration.py | Job orchestration | DAGs, scheduling, monitoring |

---

## Implementation Guide

### Step 1: Environment Setup

1. Create a Databricks workspace on AWS/Azure/GCP
2. Enable Unity Catalog
3. Create a cluster with Photon enabled (for notebooks)
4. Configure S3/ADLS bucket for checkpoints and data

---

## DATABRICKS EXECUTION GUIDE

### Prerequisites

Before executing the scripts, ensure you have:
- Databricks workspace access
- Unity Catalog enabled
- SQL Warehouse created (recommended: Medium or Large, Photon-enabled)
- S3/ADLS storage configured
- Appropriate permissions to create catalogs, schemas, and tables

---

### STEP 1: Create SQL Warehouse

#### Using Databricks UI

1. Navigate to your workspace: `https://<workspace-url>.databricks.com`
2. Click **SQL** → **SQL Warehouses** in the sidebar
3. Click **Create SQL Warehouse** button

4. Configure the sourcefuse_poc with available options (options vary by edition):

   | Setting | Value |
   |---------|-------|
   | **Name** | `sourcefuse_poc` |
   | **Size** | Select **Medium** or **Large** (for best performance) |
   | **Auto-stop** | Set to **10 minutes** (or lowest available) |
   | **Scaling** | If available, enable **Auto-scale** |
   | **Channel** | Select **Current** (unless advised otherwise) |

5. Click **Create** and wait for status to show "Running"

> **Note**: Options like "Type", "Enable Photon", and "Spot instances" may not appear depending on:
> - Your Databricks edition (Standard, Pro, Enterprise, Serverless)
> - Workspace configuration by your admin
> - Cloud provider (AWS, Azure, GCP)
>
> The sourcefuse_poc will work with the available options. For best performance, choose **Large** or **2X-Large** size.

#### Creating a Cluster (Alternative to SQL Warehouse)

If SQL Warehouse is not available or you need more control:

1. Click **Compute** → **Compute clusters** in the sidebar
2. Click **Create Compute cluster**
3. Configure:

   | Setting | Recommended Value |
   |---------|-------------------|
   | Cluster Name | `sourcefuse_poc-cluster` |
   | Databricks Runtime Version | Latest (e.g., `15.x.x`) |
   | Photon | ✓ Enable (if available) |
   | Node Type | `Memory-optimized` or similar |
   | Drivers | Single Node (for testing) or Shared (multi-node) |
   | Workers | 4-8 (or enable autoscaling) |
   | Auto Termination | 10-30 minutes |

4. Click **Create cluster**

---

### STEP 2: Import Notebooks to Databricks Workspace

#### Option A: Using Databricks UI

1. In your workspace, navigate to **Workspace**
2. Create a new folder: **Repos** → **Import Repository**
3. Import from Git:
   - Git Provider: **GitHub**
   - Repository URL: `your-repo-url`
   - Or select **Upload files** and upload the `.py` files

4. After import, your workspace should have:
   ```
   /Repos/databricks-poc/
   ├── notebooks/
   │   ├── 01_data_generation.py
   │   ├── 02_streaming_pipeline.py
   │   ├── 03_batch_etl_pipeline.py
   │   ├── 04_sql_analytics.py
   │   ├── 05_unity_catalog_governance.py
   │   └── 06_workflows_orchestration.py
   └── sql/
       └── maintenance_scripts.sql
   ```

#### Option B: Using Databricks CLI

```bash
# Import notebooks
databricks workspace import_dir ./notebooks /Repos/sourcefuse_poc/notebooks

# Import SQL scripts
databricks workspace import_dir ./sql /Repos/sourcefuse_poc/sql
```

---

### STEP 3: Update Configuration (Required)

Before running the notebooks, update the `CONFIG` dictionary at the top of each notebook:

```python
CONFIG = {
    "catalog": "sourcefuse_poc",              # Your catalog name
    "s3_bucket": "s3://databricks-storage-7474650844180840",  # UPDATE THIS with your bucket
    "num_products": 100_000,              # Adjust based on your needs
    "num_customers": 10_000_000,
    "num_transactions": 500_000_000,
}
```

---

### STEP 4: Execute Scripts in Order

> **IMPORTANT**: Execute the notebooks sequentially. Each notebook depends on the output of the previous ones.

#### Notebook 01: Data Generation

**Purpose**: Generate synthetic retail data (~50-100GB)

**Execution Steps**:
1. Open [01_data_generation.py](notebooks/01_data_generation.py)
2. Attach to your cluster (or SQL Warehouse)
3. Click **Run All** or execute cell by cell
4. **Expected runtime**: 30-60 minutes on 8-worker cluster

**What it creates**:
- Bronze layer tables: products, customers, stores, sourcefuse_pocs, transactions, inventory_snapshots
- Optimized Delta tables with Z-ORDER statistics

**Verify completion**:
```sql
USE CATALOG sourcefuse_poc;
SHOW TABLES IN bronze;
-- Should show 8 tables
```

---

#### Notebook 02: Streaming Pipeline (Optional)

**Purpose**: Demonstrate real-time streaming ingestion

**Execution Steps**:
1. Open [02_streaming_pipeline.py](notebooks/02_streaming_pipeline.py)
2. Attach to a cluster (SQL Warehouse not recommended for streaming)
3. Run the notebook
4. Streams will run continuously until manually stopped

**What it creates**:
- Bronze: `clickstream_events`
- Silver: `session_metrics`, `product_trending`, `clickstream_enriched`
- Gold: `realtime_dashboard`

**Monitor streams**:
```sql
-- Check streaming data
SELECT COUNT(*) FROM sourcefuse_poc.bronze.clickstream_events;
SELECT COUNT(*) FROM sourcefuse_poc.gold.realtime_dashboard;
```

---

#### Notebook 03: Batch ETL Pipeline

**Purpose**: Transform data through medallion architecture (Bronze → Silver → Gold)

**Execution Steps**:
1. Open [03_batch_etl_pipeline.py](notebooks/03_batch_etl_pipeline.py)
2. Attach to your cluster or SQL Warehouse
3. Click **Run All**
4. **Expected runtime**: 45-90 minutes

**What it creates**:
- Silver layer: customers (cleansed), transactions (enriched), products (enhanced)
- Gold layer: daily_sales_summary, customer_360, product_performance, store_analytics

**Verify completion**:
```sql
USE CATALOG sourcefuse_poc;
SHOW TABLES IN silver;
SHOW TABLES IN gold;
```

---

#### Notebook 04: SQL Analytics & Performance Testing

**Purpose**: Run analytics queries and benchmark performance

**Execution Steps**:
1. Open [04_sql_analytics.py](notebooks/04_sql_analytics.py)
2. **Must use SQL Warehouse** (Photon-enabled recommended)
3. Run individual query cells to test performance
4. Review benchmark results

**Key queries included**:
- Daily sales trends with rolling averages
- Customer cohort analysis
- Product affinity (market basket analysis)
- Inventory optimization recommendations

---

#### Notebook 05: Unity Catalog Governance

**Purpose**: Set up data governance, access controls, and tagging

**Execution Steps**:
1. Open [05_unity_catalog_governance.py](notebooks/05_unity_catalog_governance.py)
2. Run cell by cell to configure governance
3. Update group names to match your workspace groups

**What it configures**:
- Table and column-level access controls
- PII masking views
- Data tags (domain, sensitivity, PII)
- Table documentation/comments
- Data quality constraints

**Verify governance**:
```sql
-- View table tags
SELECT * FROM system.information_schema.table_tags
WHERE table_catalog = 'sourcefuse_poc';

-- View column tags
SELECT * FROM system.information_schema.column_tags
WHERE table_catalog = 'sourcefuse_poc';
```

---

#### Notebook 06: Workflows Orchestration

**Purpose**: Define job configurations for automated execution

**Execution Steps**:
1. Open [06_workflows_orchestration.py](notebooks/06_workflows_orchestration.py)
2. Review workflow configurations
3. Create workflows via UI or API (code provided in notebook)

**Workflows defined**:
- Daily ETL Pipeline (scheduled)
- Streaming Pipeline (continuous)
- Weekly Maintenance (VACUUM, OPTIMIZE)

---

### STEP 5: Run SQL Maintenance Scripts

#### Option A: Using SQL Editor

1. Navigate to **SQL** → **SQL Editor**
2. Create a new query
3. Select your SQL Warehouse
4. Copy and execute sections from [maintenance_scripts.sql](sql/maintenance_scripts.sql)

#### Option B: Using Databricks CLI

```bash
# Execute maintenance scripts
databricks sql execute-sourcefuse_poc \
  --sourcefuse_poc-id <your-sourcefuse_poc-id> \
  --file sql/maintenance_scripts.sql
```

**Maintenance script sections**:

| Section | Purpose | Frequency |
|---------|---------|-----------|
| Table Optimization | OPTIMIZE + Z-ORDER | Daily |
| Statistics | ANALYZE TABLE | Daily |
| VACUUM | Remove old files | Weekly |
| Materialized Views | REFRESH MV | Daily |
| Data Quality | Validation checks | Daily |

---

### STEP 6: Verify Implementation

Run these verification queries:

```sql
-- Check all tables were created
USE CATALOG sourcefuse_poc;

SELECT
    table_schema,
    COUNT(*) as table_count
FROM information_schema.tables
WHERE table_catalog = 'sourcefuse_poc'
GROUP BY table_schema
ORDER BY table_schema;

-- Verify row counts
SELECT 'customers' as table_name, COUNT(*) as row_count FROM bronze.customers
UNION ALL
SELECT 'transactions', COUNT(*) FROM bronze.transactions
UNION ALL
SELECT 'products', COUNT(*) FROM bronze.products;

-- Check data quality
SELECT
    value_segment,
    COUNT(*) as customers,
    AVG(total_spend) as avg_spend
FROM gold.customer_360
GROUP BY value_segment
ORDER BY avg_spend DESC;
```

---

### Execution Summary

| Step | Notebook | Runtime | Dependencies |
|------|----------|---------|--------------|
| 1 | 01_data_generation.py | 30-60 min | None |
| 2 | 02_streaming_pipeline.py | Continuous | Step 1 |
| 3 | 03_batch_etl_pipeline.py | 45-90 min | Step 1 |
| 4 | 04_sql_analytics.py | 10-20 min | Steps 1-3 |
| 5 | 05_unity_catalog_governance.py | 5-10 min | Steps 1-3 |
| 6 | 06_workflows_orchestration.py | Setup only | Steps 1-3 |

---

### Quick Start (Minimal Configuration)

For a quick demo with smaller data:

```python
# In 01_data_generation.py, update CONFIG:
CONFIG = {
    "catalog": "sourcefuse_poc",
    "num_products": 10_000,      # Reduced from 100K
    "num_customers": 1_000_000,  # Reduced from 10M
    "num_transactions": 50_000_000,  # Reduced from 500M
    "partitions": 50,            # Reduced from 200
}
```

This will complete in ~10-15 minutes total.

---

### Troubleshooting Execution Issues

**Issue**: "Table not found" errors
**Solution**: Ensure notebooks are run in order (01 → 02 → 03)

**Issue**: Out of memory errors
**Solution**: Use a larger cluster or reduce data volumes in CONFIG

**Issue**: SQL Warehouse timeout
**Solution**: Increase sourcefuse_poc size or break queries into smaller batches

**Issue**: Streaming jobs falling behind
**Solution**: Increase cluster workers or reduce events_per_second

---

## Configuration Reference

### Cluster Configuration (for notebooks)

**Minimum for POC**:
- Runtime: Latest LTS version
- Workers: 2-4 (Single Node or Small Cluster)
- Auto-termination: 10-20 minutes

**Recommended for Production**:
```json
{
  "cluster_name": "sourcefuse_poc-cluster",
  "spark_version": "15.x LTS",
  "node_type": "Memory Optimized",
  "num_workers": 8,
  "autoscale": {"min_workers": 4, "max_workers": 16},
  "photon": true
}
```

### SQL Warehouse Configuration

**Minimum for POC**:
- Size: Small or Medium
- Auto-stop: 10 minutes

**Recommended for Analytics**:
- Size: Large or 2X-Large
- Auto-stop: 10 minutes
- Auto-scale: Enabled (if available)

> **Note**: Available options depend on your Databricks edition:
> - **Standard**: Basic SQL Warehouse
> - **Pro/Enterprise**: Advanced options (Photon, scaling, spot instances)
> - **Serverless**: Managed compute with automatic scaling

---

## Edition Comparison

| Feature | Standard | Pro | Enterprise | Serverless |
|---------|----------|-----|------------|------------|
| SQL Warehouse | ✓ | ✓ | ✓ | ✓ |
| Photon | ✗ | ✓ | ✓ | ✓ |
| Auto-scaling | ✗ | ✓ | ✓ | Automatic |
| Spot Instances | ✗ | ✓ | ✓ | N/A |
| Unity Catalog | ✗ | ✗ | ✓ | ✓ |

**For this POC**: Standard or Pro edition is sufficient. Enterprise is recommended for Unity Catalog governance features.

---

## Success Criteria Checklist

After executing all scripts, verify:

- [ ] Bronze layer has 8 tables with expected row counts
- [ ] Silver layer has 3 tables (customers, transactions, products)
- [ ] Gold layer has 4 tables (daily_sales_summary, customer_360, product_performance, store_analytics)
- [ ] All tables have OPTIMIZE and Z-ORDER applied
- [ ] Statistics computed via ANALYZE TABLE
- [ ] Tags applied to key tables
- [ ] At least one materialized view created
- [ ] Data quality queries return zero violations

---

## Performance Benchmarks

### Expected Results

| Query Type | Data Size | Target Time | Photon Improvement |
|------------|-----------|-------------|-------------------|
| Simple Aggregation | 500M rows | < 5 seconds | 3-5x faster |
| Date Range Scan | 30 days | < 10 seconds | 2-4x faster |
| Large Join (3 tables) | 500M x 10M | < 30 seconds | 4-8x faster |
| Complex Multi-Join | 1B+ rows | < 60 seconds | 3-6x faster |
| Window Functions | 100M rows | < 15 seconds | 2-4x faster |

### Streaming Throughput

| Metric | Target | Notes |
|--------|--------|-------|
| Events/second | 50,000-100,000 | Sustained throughput |
| End-to-end latency | < 30 seconds | Event to queryable |
| Checkpoint overhead | < 5% | Exactly-once guarantee |

---

## Cost Optimization Tips

1. **Use Spot Instances**: 60-80% cost savings for batch workloads
2. **Right-size Clusters**: Start small, scale based on metrics
3. **Auto-terminate**: Set 10-15 minute idle timeout
4. **Job Clusters**: Use job clusters instead of all-purpose for production
5. **Delta Optimization**: OPTIMIZE and Z-ORDER reduce scan costs
6. **Partitioning**: Partition by date for time-series queries
7. **Materialized Views**: Pre-compute expensive aggregations

---

## Troubleshooting Guide

### Common Issues

**Issue**: Streaming job falls behind
**Solution**: Increase cluster size, check for data skew, optimize aggregations

**Issue**: OPTIMIZE taking too long
**Solution**: Run incrementally with WHERE clause, use Z-ORDER only on high-cardinality columns

**Issue**: Query performance degraded
**Solution**: Run ANALYZE TABLE, check for small files, verify Photon is enabled

**Issue**: Unity Catalog permissions not working
**Solution**: Verify group membership, check inheritance, review audit logs

---

## Success Criteria Checklist

- [ ] 500M+ transactions generated successfully
- [ ] Streaming maintains 50K events/second for 1 hour
- [ ] Batch ETL completes in under 2 hours
- [ ] SQL queries return in under 30 seconds
- [ ] Concurrency test passes with 10 users
- [ ] Unity Catalog lineage visible
- [ ] Workflows execute on schedule
- [ ] All gold tables populated correctly

---

## Next Steps

1. **Production Deployment**: Migrate to production workspace
2. **BI Integration**: Connect Tableau/Power BI to SQL Warehouse
3. **ML Workloads**: Add MLflow model training
4. **Cost Analysis**: Review actual vs. estimated costs
5. **Performance Tuning**: Fine-tune based on production patterns

---

## Support Resources

- [Databricks Documentation](https://docs.databricks.com)
- [Delta Lake Documentation](https://docs.delta.io)
- [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Photon Performance Guide](https://docs.databricks.com/runtime/photon.html)

---

*Document Version: 1.0*
*Last Updated: January 2026*
