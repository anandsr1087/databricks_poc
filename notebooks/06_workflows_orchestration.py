# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Databricks Workflows Orchestration
# MAGIC
# MAGIC This notebook demonstrates workflow orchestration using:
# MAGIC - **Databricks Workflows** for job orchestration
# MAGIC - **Task dependencies** and parallel execution
# MAGIC - **Job clusters** vs **All-purpose clusters**
# MAGIC - **Parameterization** and **dynamic configuration**
# MAGIC - **Error handling** and **retry logic**
# MAGIC - **Monitoring** and **alerting**
# MAGIC
# MAGIC This notebook provides workflow configurations to be used in the Databricks Workflows UI or via the Jobs API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    RETAIL POC WORKFLOW                         │
# MAGIC ├─────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                 │
# MAGIC │   ┌───────────────┐                                            │
# MAGIC │   │ Data          │                                            │
# MAGIC │   │ Generation    │ (Run once for initial setup)              │
# MAGIC │   └───────┬───────┘                                            │
# MAGIC │           │                                                    │
# MAGIC │           ▼                                                    │
# MAGIC │   ┌───────────────────────────────────────────────────┐       │
# MAGIC │   │              DAILY ETL PIPELINE                    │       │
# MAGIC │   │  ┌─────────┐  ┌─────────┐  ┌─────────┐           │       │
# MAGIC │   │  │ Bronze  │──│ Silver  │──│  Gold   │           │       │
# MAGIC │   │  │   ETL   │  │   ETL   │  │   ETL   │           │       │
# MAGIC │   │  └─────────┘  └─────────┘  └─────────┘           │       │
# MAGIC │   └───────────────────────────────────────────────────┘       │
# MAGIC │           │                                                    │
# MAGIC │           ▼                                                    │
# MAGIC │   ┌───────────────────────────────────────────────────┐       │
# MAGIC │   │            PARALLEL TASKS                          │       │
# MAGIC │   │  ┌─────────┐  ┌─────────┐  ┌─────────┐           │       │
# MAGIC │   │  │ SQL     │  │ Data    │  │ Notify  │           │       │
# MAGIC │   │  │ Refresh │  │ Quality │  │ & Alert │           │       │
# MAGIC │   │  └─────────┘  └─────────┘  └─────────┘           │       │
# MAGIC │   └───────────────────────────────────────────────────┘       │
# MAGIC │                                                                 │
# MAGIC │   ┌───────────────┐                                            │
# MAGIC │   │ Streaming     │ (Continuous - separate workflow)          │
# MAGIC │   │ Pipeline      │                                            │
# MAGIC │   └───────────────┘                                            │
# MAGIC │                                                                 │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow 1: Daily ETL Pipeline (JSON Configuration)

# COMMAND ----------

import json

# Daily ETL Workflow Configuration
daily_etl_workflow = {
    "name": "sourcefuse_poc_daily_etl",
    "description": "Daily ETL pipeline for retail POC - Bronze → Silver → Gold",
    "email_notifications": {
        "on_failure": ["data-team@company.com"],
        "on_success": ["data-team@company.com"],
        "no_alert_for_skipped_runs": True
    },
    "webhook_notifications": {
        "on_failure": [{"id": "slack-webhook-id"}]  # Replace with actual webhook
    },
    "timeout_seconds": 7200,  # 2 hour timeout
    "max_concurrent_runs": 1,
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",  # Daily at 6 AM UTC
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED"
    },
    "job_clusters": [
        {
            "job_cluster_key": "etl_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "auto",
                    "spot_bid_price_percent": 100
                },
                "node_type_id": "r5d.4xlarge",  # Memory-optimized for ETL
                "num_workers": 8,
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                    "spark.databricks.photon.enabled": "true"
                },
                "custom_tags": {
                    "project": "sourcefuse_poc",
                    "environment": "production"
                }
            }
        },
        {
            "job_cluster_key": "sql_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-photon-scala2.12",  # Photon for SQL
                "aws_attributes": {
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "auto"
                },
                "node_type_id": "r5d.2xlarge",
                "num_workers": 4,
                "spark_conf": {
                    "spark.databricks.photon.enabled": "true"
                }
            }
        }
    ],
    "tasks": [
        {
            "task_key": "bronze_to_silver_customers",
            "description": "Transform bronze customers to silver",
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/03_batch_etl_pipeline",
                "base_parameters": {
                    "layer": "silver",
                    "table": "customers"
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 1800,
            "retry_on_timeout": True,
            "max_retries": 2,
            "min_retry_interval_millis": 60000
        },
        {
            "task_key": "bronze_to_silver_transactions",
            "description": "Transform bronze transactions to silver",
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/03_batch_etl_pipeline",
                "base_parameters": {
                    "layer": "silver",
                    "table": "transactions"
                }
            },
            "timeout_seconds": 3600,
            "max_retries": 2
        },
        {
            "task_key": "bronze_to_silver_products",
            "description": "Transform bronze products to silver",
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/03_batch_etl_pipeline",
                "base_parameters": {
                    "layer": "silver",
                    "table": "products"
                }
            },
            "timeout_seconds": 1800,
            "max_retries": 2
        },
        {
            "task_key": "silver_to_gold_daily_sales",
            "description": "Create gold daily sales summary",
            "depends_on": [
                {"task_key": "bronze_to_silver_transactions"}
            ],
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/03_batch_etl_pipeline",
                "base_parameters": {
                    "layer": "gold",
                    "table": "daily_sales_summary"
                }
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "silver_to_gold_customer_360",
            "description": "Create gold customer 360",
            "depends_on": [
                {"task_key": "bronze_to_silver_customers"},
                {"task_key": "bronze_to_silver_transactions"}
            ],
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/03_batch_etl_pipeline",
                "base_parameters": {
                    "layer": "gold",
                    "table": "customer_360"
                }
            },
            "timeout_seconds": 2400
        },
        {
            "task_key": "silver_to_gold_product_performance",
            "description": "Create gold product performance",
            "depends_on": [
                {"task_key": "bronze_to_silver_products"},
                {"task_key": "bronze_to_silver_transactions"}
            ],
            "job_cluster_key": "etl_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/03_batch_etl_pipeline",
                "base_parameters": {
                    "layer": "gold",
                    "table": "product_performance"
                }
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "optimize_tables",
            "description": "Run OPTIMIZE and ANALYZE on all tables",
            "depends_on": [
                {"task_key": "silver_to_gold_daily_sales"},
                {"task_key": "silver_to_gold_customer_360"},
                {"task_key": "silver_to_gold_product_performance"}
            ],
            "job_cluster_key": "sql_cluster",
            "sql_task": {
                "warehouse_id": "your-warehouse-id",  # Replace with actual warehouse ID
                "query": {
                    "query_id": "optimize-query-id"  # Or use file path
                }
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "data_quality_checks",
            "description": "Run data quality validation",
            "depends_on": [
                {"task_key": "optimize_tables"}
            ],
            "job_cluster_key": "sql_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/07_data_quality_checks",
                "base_parameters": {
                    "fail_on_error": "true"
                }
            },
            "timeout_seconds": 900
        },
        {
            "task_key": "refresh_materialized_views",
            "description": "Refresh all materialized views",
            "depends_on": [
                {"task_key": "data_quality_checks"}
            ],
            "job_cluster_key": "sql_cluster",
            "sql_task": {
                "warehouse_id": "your-warehouse-id",
                "query": {
                    "query_id": "refresh-mvs-query-id"
                }
            },
            "timeout_seconds": 600
        }
    ],
    "run_as": {
        "service_principal_name": "sourcefuse_poc-service-principal"  # Use service principal
    },
    "tags": {
        "project": "sourcefuse_poc",
        "team": "data_engineering",
        "cost_center": "DE-001"
    }
}

print("Daily ETL Workflow Configuration:")
print(json.dumps(daily_etl_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow 2: Streaming Pipeline (Delta Live Tables Alternative)

# COMMAND ----------

streaming_workflow = {
    "name": "sourcefuse_poc_streaming",
    "description": "Continuous streaming pipeline for clickstream data",
    "continuous": True,  # This makes it a continuous job
    "max_concurrent_runs": 1,
    "job_clusters": [
        {
            "job_cluster_key": "streaming_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "aws_attributes": {
                    "availability": "ON_DEMAND",  # On-demand for stability
                    "zone_id": "auto"
                },
                "node_type_id": "r5d.2xlarge",
                "num_workers": 4,
                "autoscale": {
                    "min_workers": 2,
                    "max_workers": 8
                },
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.sql.streaming.schemaInference": "true"
                }
            }
        }
    ],
    "tasks": [
        {
            "task_key": "clickstream_ingestion",
            "job_cluster_key": "streaming_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/02_streaming_pipeline",
                "base_parameters": {
                    "events_per_second": "50000",
                    "checkpoint_path": "s3://databricks-storage-7474650844180840/checkpoints/clickstream"
                }
            }
        }
    ],
    "email_notifications": {
        "on_failure": ["data-team@company.com", "oncall@company.com"]
    },
    "tags": {
        "project": "sourcefuse_poc",
        "workload": "streaming"
    }
}

print("\nStreaming Workflow Configuration:")
print(json.dumps(streaming_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow 3: Weekly Maintenance

# COMMAND ----------

maintenance_workflow = {
    "name": "sourcefuse_poc_weekly_maintenance",
    "description": "Weekly maintenance tasks - VACUUM, OPTIMIZE, statistics refresh",
    "schedule": {
        "quartz_cron_expression": "0 0 2 ? * SUN",  # Sunday at 2 AM
        "timezone_id": "UTC"
    },
    "job_clusters": [
        {
            "job_cluster_key": "maintenance_cluster",
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "r5d.xlarge",
                "num_workers": 2,
                "spark_conf": {
                    "spark.databricks.delta.retentionDurationCheck.enabled": "false"
                }
            }
        }
    ],
    "tasks": [
        {
            "task_key": "vacuum_bronze",
            "job_cluster_key": "maintenance_cluster",
            "sql_task": {
                "warehouse_id": "your-warehouse-id",
                "query": {
                    "query_id": "vacuum-bronze-tables"
                }
            }
        },
        {
            "task_key": "vacuum_silver",
            "depends_on": [{"task_key": "vacuum_bronze"}],
            "job_cluster_key": "maintenance_cluster",
            "sql_task": {
                "warehouse_id": "your-warehouse-id",
                "query": {
                    "query_id": "vacuum-silver-tables"
                }
            }
        },
        {
            "task_key": "vacuum_gold",
            "depends_on": [{"task_key": "vacuum_silver"}],
            "job_cluster_key": "maintenance_cluster",
            "sql_task": {
                "warehouse_id": "your-warehouse-id",
                "query": {
                    "query_id": "vacuum-gold-tables"
                }
            }
        },
        {
            "task_key": "analyze_all_tables",
            "depends_on": [{"task_key": "vacuum_gold"}],
            "job_cluster_key": "maintenance_cluster",
            "notebook_task": {
                "notebook_path": "/Repos/sourcefuse_poc/notebooks/maintenance/analyze_tables"
            }
        }
    ]
}

print("\nWeekly Maintenance Workflow Configuration:")
print(json.dumps(maintenance_workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Notebooks for Modular Tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameterized ETL Task Template

# COMMAND ----------

# DBTITLE 1,Cell 11
# This would be in a separate notebook (e.g., etl_task_template.py)
# Demonstrates how to make notebooks workflow-ready with parameters

etl_template = '''
# Get task parameters
dbutils.widgets.text("layer", "silver", "Target Layer")
dbutils.widgets.text("table", "", "Table Name")
dbutils.widgets.text("run_date", "", "Run Date (YYYY-MM-DD)")

layer = dbutils.widgets.get("layer")
table = dbutils.widgets.get("table")
run_date = dbutils.widgets.get("run_date") or str(date.today())

print(f"Processing: {layer}.{table} for {run_date}")
'''

print("ETL Template Notebook:")
print(etl_template)

# COMMAND ----------

# DBTITLE 1,Cell 12
# Define ETL logic based on parameters
from datetime import date, datetime
from pyspark.sql import functions as F

# Get parameters from widgets (for workflow execution)
# For demonstration, set default values
try:
    dbutils.widgets.text("layer", "silver", "Target Layer")
    dbutils.widgets.text("table", "customers", "Table Name")
    layer = dbutils.widgets.get("layer")
    table = dbutils.widgets.get("table")
except:
    # Fallback for non-workflow execution
    layer = "silver"
    table = "customers"

catalog = "sourcefuse_poc"

if layer == "silver" and table == "customers":
    # Silver customers ETL
    bronze_df = spark.table(f"{catalog}.bronze.customers")
    
    silver_df = (
        bronze_df
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("dq_score", F.lit(100.0))  # Use DOUBLE to match table schema
        .withColumn("silver_processed_at", F.current_timestamp())
    )
    
    silver_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"{catalog}.silver.customers")
    
    print(f"✅ Processed {silver_df.count():,} customers")

elif layer == "gold" and table == "customer_360":
    # Gold customer 360 ETL
    # ... implementation
    pass

else:
    print(f"⚠️ No ETL logic defined for {layer}.{table}")

# COMMAND ----------

# DBTITLE 1,Cell 13
from datetime import date, datetime
# Return success status for workflow
dbutils.notebook.exit(json.dumps({
    "status": "success",
    "layer": layer,
    "table": table,
    "rows_processed": silver_df.count() if 'silver_df' in dir() else 0,
    "timestamp": str(datetime.now())
}))

print("ETL Template Notebook:")
print(etl_template)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Workflows via API

# COMMAND ----------

def create_workflow_via_api(workflow_config, host, token):
    """
    Create or update a Databricks workflow via the Jobs API
    
    Args:
        workflow_config: Job configuration dictionary
        host: Databricks workspace URL
        token: Databricks personal access token
    """
    import requests
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Check if job exists
    list_url = f"{host}/api/2.1/jobs/list"
    response = requests.get(list_url, headers=headers, params={"name": workflow_config["name"]})
    
    if response.status_code == 200:
        jobs = response.json().get("jobs", [])
        existing_job = next((j for j in jobs if j["settings"]["name"] == workflow_config["name"]), None)
        
        if existing_job:
            # Update existing job
            update_url = f"{host}/api/2.1/jobs/update"
            payload = {
                "job_id": existing_job["job_id"],
                "new_settings": workflow_config
            }
            response = requests.post(update_url, headers=headers, json=payload)
            print(f"Updated job: {workflow_config['name']} (ID: {existing_job['job_id']})")
        else:
            # Create new job
            create_url = f"{host}/api/2.1/jobs/create"
            response = requests.post(create_url, headers=headers, json=workflow_config)
            job_id = response.json().get("job_id")
            print(f"Created job: {workflow_config['name']} (ID: {job_id})")
    
    return response.json()

# Example usage (uncomment and configure):
# host = "https://your-workspace.cloud.databricks.com"
# token = dbutils.secrets.get("secrets-scope", "api-token")
# result = create_workflow_via_api(daily_etl_workflow, host, token)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Observability

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Run Metrics Collection

# COMMAND ----------

def collect_job_metrics(job_id, host, token):
    """Collect metrics from recent job runs for analysis"""
    import requests
    
    headers = {"Authorization": f"Bearer {token}"}
    
    # Get recent runs
    url = f"{host}/api/2.1/jobs/runs/list"
    params = {"job_id": job_id, "limit": 50}
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        runs = response.json().get("runs", [])
        
        metrics = []
        for run in runs:
            metrics.append({
                "run_id": run["run_id"],
                "start_time": run.get("start_time"),
                "end_time": run.get("end_time"),
                "duration_seconds": (run.get("end_time", 0) - run.get("start_time", 0)) / 1000,
                "state": run.get("state", {}).get("result_state"),
                "life_cycle_state": run.get("state", {}).get("life_cycle_state")
            })
        
        return metrics
    
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom Alerting Logic

# COMMAND ----------

def send_alert(message, channel="slack", webhook_url=None):
    """Send alerts to various channels"""
    import requests
    import json
    
    if channel == "slack" and webhook_url:
        payload = {
            "text": message,
            "attachments": [{
                "color": "#ff0000" if "FAILED" in message else "#00ff00",
                "title": "Databricks Job Alert",
                "text": message
            }]
        }
        requests.post(webhook_url, json=payload)
    
    elif channel == "teams" and webhook_url:
        payload = {
            "@type": "MessageCard",
            "title": "Databricks Job Alert",
            "text": message
        }
        requests.post(webhook_url, json=payload)
    
    print(f"Alert sent to {channel}: {message[:100]}...")

# Example: Alert on SLA breach
def check_sla_and_alert(job_name, max_duration_minutes, actual_duration_minutes, webhook_url):
    """Check if job exceeded SLA and send alert"""
    if actual_duration_minutes > max_duration_minutes:
        message = f"⚠️ SLA BREACH: Job '{job_name}' took {actual_duration_minutes:.1f} minutes (SLA: {max_duration_minutes} min)"
        send_alert(message, "slack", webhook_url)
        return True
    return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Best Practices Summary
# MAGIC
# MAGIC 1. **Use Job Clusters**: Cheaper than all-purpose clusters, isolated environments
# MAGIC 2. **Parameterize Notebooks**: Make tasks reusable with widgets/parameters
# MAGIC 3. **Implement Retries**: Configure retry logic for transient failures
# MAGIC 4. **Set Timeouts**: Prevent runaway jobs from consuming resources
# MAGIC 5. **Use Service Principals**: Run jobs as service accounts, not users
# MAGIC 6. **Monitor SLAs**: Track job durations and alert on breaches
# MAGIC 7. **Tag Everything**: Use consistent tags for cost tracking
# MAGIC 8. **Parallel When Possible**: Design DAGs to maximize parallelism
# MAGIC 9. **Separate Concerns**: Different clusters for different workload types
# MAGIC 10. **Version Control**: Keep workflow configs in source control