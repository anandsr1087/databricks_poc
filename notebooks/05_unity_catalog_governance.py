# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Unity Catalog Governance
# MAGIC
# MAGIC This notebook demonstrates Unity Catalog features for enterprise governance:
# MAGIC - **Catalog/Schema/Table hierarchy** management
# MAGIC - **Fine-grained access control** (table, column, row-level)
# MAGIC - **Data lineage** tracking
# MAGIC - **Data discovery** and tagging
# MAGIC - **Audit logging** and compliance
# MAGIC
# MAGIC **Prerequisite**: Unity Catalog must be enabled in your workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CONFIG = {
    "catalog": "sourcefuse_poc",
    "admin_group": "data_admins",
    "analyst_group": "data_analysts",
    "scientist_group": "data_scientists"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Catalog Structure Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View catalog structure
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View schemas in our catalog
# MAGIC SHOW SCHEMAS IN sourcefuse_poc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all tables in gold schema
# MAGIC SHOW TABLES IN sourcefuse_poc.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Access Control Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Catalog-Level Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant usage on catalog to analyst group
# MAGIC -- Note: Replace 'data_analysts' with your actual group name
# MAGIC -- GRANT USAGE ON CATALOG sourcefuse_poc TO `data_analysts`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema-Level Access Control

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Analysts can read from gold, not bronze
# MAGIC -- GRANT USAGE ON SCHEMA sourcefuse_poc.gold TO `data_analysts`;
# MAGIC -- GRANT SELECT ON SCHEMA sourcefuse_poc.gold TO `data_analysts`;
# MAGIC
# MAGIC -- Data scientists get silver access too
# MAGIC -- GRANT USAGE ON SCHEMA sourcefuse_poc.silver TO `data_scientists`;
# MAGIC -- GRANT SELECT ON SCHEMA sourcefuse_poc.silver TO `data_scientists`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table-Level Permissions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show current grants on a table
# MAGIC SHOW GRANTS ON TABLE sourcefuse_poc.gold.customer_360;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column-Level Security (Dynamic Views)

# COMMAND ----------

# DBTITLE 1,Cell 16
# MAGIC %sql
# MAGIC     
# MAGIC -- Create a secure view that masks PII based on user group
# MAGIC CREATE OR REPLACE VIEW sourcefuse_poc.gold.v_customer_360_secure AS
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     -- Mask email for non-admin users
# MAGIC     CASE 
# MAGIC         WHEN is_account_group_member('data_admins') THEN c.email
# MAGIC         ELSE CONCAT(SUBSTRING(c.email, 1, 3), '***@', SPLIT(c.email, '@')[1])
# MAGIC     END as email,
# MAGIC     -- Mask phone for non-admin users
# MAGIC     CASE 
# MAGIC         WHEN is_account_group_member('data_admins') THEN c.phone
# MAGIC         ELSE CONCAT('***-***-', RIGHT(c.phone, 4))
# MAGIC     END as phone,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.state,
# MAGIC     c.customer_segment,
# MAGIC     c.tenure_category,
# MAGIC     -- These metrics are fine for all users
# MAGIC     c360.total_transactions,
# MAGIC     c360.total_spend,
# MAGIC     c360.avg_order_value,
# MAGIC     c360.rfm_score,
# MAGIC     c360.value_segment,
# MAGIC     c360.preferred_channel
# MAGIC FROM sourcefuse_poc.silver.customers c
# MAGIC JOIN sourcefuse_poc.gold.customer_360 c360 ON c.customer_id = c360.customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row-Level Security

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a row-level security view
# MAGIC -- Regional managers only see their region's data
# MAGIC CREATE OR REPLACE VIEW sourcefuse_poc.gold.v_store_analytics_secure AS
# MAGIC SELECT *
# MAGIC FROM sourcefuse_poc.gold.store_analytics
# MAGIC WHERE 
# MAGIC     -- Admins see all
# MAGIC     is_account_group_member('data_admins')
# MAGIC     -- Or user is in the regional group for this state
# MAGIC     OR is_account_group_member(CONCAT('region_', LOWER(state)));

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Data Tagging and Classification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Tags to Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag tables with metadata
# MAGIC ALTER TABLE sourcefuse_poc.gold.customer_360 
# MAGIC SET TAGS ('domain' = 'customer', 'sensitivity' = 'high', 'pii' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sourcefuse_poc.gold.daily_sales_summary 
# MAGIC SET TAGS ('domain' = 'sales', 'sensitivity' = 'medium', 'pii' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sourcefuse_poc.gold.product_performance 
# MAGIC SET TAGS ('domain' = 'product', 'sensitivity' = 'low', 'pii' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sourcefuse_poc.gold.store_analytics 
# MAGIC SET TAGS ('domain' = 'store', 'sensitivity' = 'medium', 'pii' = 'false');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Column-Level Tags

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tag PII columns
# MAGIC ALTER TABLE sourcefuse_poc.silver.customers 
# MAGIC ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sourcefuse_poc.silver.customers 
# MAGIC ALTER COLUMN phone SET TAGS ('pii' = 'true', 'pii_type' = 'phone');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sourcefuse_poc.silver.customers 
# MAGIC ALTER COLUMN first_name SET TAGS ('pii' = 'true', 'pii_type' = 'name');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sourcefuse_poc.silver.customers 
# MAGIC ALTER COLUMN last_name SET TAGS ('pii' = 'true', 'pii_type' = 'name');

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Table Comments

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE sourcefuse_poc.gold.customer_360 IS 
# MAGIC 'Customer 360 view containing comprehensive customer analytics including RFM scores, 
# MAGIC lifetime value predictions, and behavioral segmentation. Contains PII - access restricted.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE sourcefuse_poc.gold.daily_sales_summary IS 
# MAGIC 'Daily aggregated sales metrics by channel and store. Used for executive dashboards and trend analysis.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE sourcefuse_poc.gold.product_performance IS 
# MAGIC 'Product-level performance metrics including revenue, units sold, and category rankings.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Data Lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Table Lineage (UI Feature)
# MAGIC
# MAGIC Navigate to **Data Explorer** → Select a table → Click **Lineage** tab
# MAGIC
# MAGIC This shows:
# MAGIC - Upstream dependencies (source tables)
# MAGIC - Downstream dependencies (derived tables)
# MAGIC - Jobs/notebooks that read/write the table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table dependencies programmatically
# MAGIC -- This shows the Delta table history including what modified it
# MAGIC DESCRIBE HISTORY sourcefuse_poc.gold.customer_360;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View detailed table properties
# MAGIC DESCRIBE EXTENDED sourcefuse_poc.gold.customer_360;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Data Quality Constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add NOT NULL constraint
# MAGIC ALTER TABLE sourcefuse_poc.silver.customers 
# MAGIC ALTER COLUMN customer_id SET NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add CHECK constraint for data quality
# MAGIC ALTER TABLE sourcefuse_poc.silver.customers 
# MAGIC ADD CONSTRAINT valid_dq_score CHECK (dq_score >= 0 AND dq_score <= 100);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add constraint for transaction amounts
# MAGIC ALTER TABLE sourcefuse_poc.silver.transactions 
# MAGIC ADD CONSTRAINT positive_amount CHECK (transaction_amount >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Audit and Compliance

# COMMAND ----------

# MAGIC %md
# MAGIC ### System Tables for Audit (Requires System Tables Enabled)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query audit logs for table access (if system tables enabled)
# MAGIC -- SELECT * FROM system.access.audit
# MAGIC -- WHERE table_full_name LIKE 'sourcefuse_poc.%'
# MAGIC -- AND event_time >= DATE_SUB(CURRENT_DATE(), 7)
# MAGIC -- ORDER BY event_time DESC
# MAGIC -- LIMIT 100;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Retention and GDPR Compliance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable deletion vectors for efficient GDPR deletes
# MAGIC ALTER TABLE sourcefuse_poc.silver.customers 
# MAGIC SET TBLPROPERTIES (delta.enableDeletionVectors = true);

# COMMAND ----------

# Example: GDPR Right to be Forgotten - Delete customer data
def gdpr_delete_customer(customer_id):
    """
    Delete all data for a customer across all tables (GDPR compliance)
    """
    tables_with_customer_data = [
        ("silver.customers", "customer_id"),
        ("silver.transactions", "customer_id"),
        ("gold.customer_360", "customer_id"),
    ]
    
    for table, column in tables_with_customer_data:
        full_table = f"sourcefuse_poc.{table}"
        print(f"Deleting customer {customer_id} from {full_table}...")
        
        spark.sql(f"""
            DELETE FROM {full_table} 
            WHERE {column} = {customer_id}
        """)
        
        print(f"  ✓ Deleted from {full_table}")
    
    print(f"\n✅ Customer {customer_id} data deleted from all tables")

# Example usage (commented out):
# gdpr_delete_customer(12345)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Data Discovery

# COMMAND ----------

# DBTITLE 1,Cell 49
# MAGIC %sql
# MAGIC     
# MAGIC -- Find all tables with PII
# MAGIC SELECT 
# MAGIC     catalog_name,
# MAGIC     schema_name,
# MAGIC     table_name,
# MAGIC     tag_name,
# MAGIC     tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE catalog_name = 'sourcefuse_poc'
# MAGIC   AND tag_name = 'pii'
# MAGIC   AND tag_value = 'true';

# COMMAND ----------

# DBTITLE 1,Cell 50
# MAGIC %sql
# MAGIC     
# MAGIC -- Find all columns tagged as PII
# MAGIC SELECT 
# MAGIC     catalog_name,
# MAGIC     schema_name,
# MAGIC     table_name,
# MAGIC     column_name,
# MAGIC     tag_name,
# MAGIC     tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE catalog_name = 'sourcefuse_poc'
# MAGIC   AND tag_name = 'pii';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Search for tables by domain
# MAGIC SELECT 
# MAGIC     schema_name,
# MAGIC     table_name,
# MAGIC     tag_value as domain
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE catalog_name = 'sourcefuse_poc'
# MAGIC   AND tag_name = 'domain'
# MAGIC ORDER BY tag_value, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Governance Dashboard Summary

# COMMAND ----------

# Generate governance summary report
print("=" * 80)
print("UNITY CATALOG GOVERNANCE SUMMARY")
print("=" * 80)

# Count objects
catalogs = spark.sql("SHOW CATALOGS").count()
schemas = spark.sql("SHOW SCHEMAS IN sourcefuse_poc").count()
tables = spark.sql("""
    SELECT COUNT(*) as cnt FROM (
        SELECT table_name FROM sourcefuse_poc.information_schema.tables 
        WHERE table_schema IN ('bronze', 'silver', 'gold')
    )
""").collect()[0]['cnt']

print(f"\nCatalog Objects:")
print(f"  Catalogs: {catalogs}")
print(f"  Schemas in sourcefuse_poc: {schemas}")
print(f"  Tables (bronze/silver/gold): {tables}")

# COMMAND ----------

# DBTITLE 1,Cell 54
# Check for tables with tags
tagged_tables = spark.sql("""
    SELECT 
        tag_name,
        COUNT(DISTINCT CONCAT(schema_name, '.', table_name)) as table_count
    FROM system.information_schema.table_tags
    WHERE catalog_name = 'sourcefuse_poc'
    GROUP BY tag_name
""")

print("\nTable Tags Summary:")
for row in tagged_tables.collect():
    print(f"  {row['tag_name']}: {row['table_count']} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance Best Practices Checklist
# MAGIC
# MAGIC - [ ] **Catalog Structure**: Organize by environment (dev/staging/prod) or domain
# MAGIC - [ ] **Access Control**: Use groups, not individual users
# MAGIC - [ ] **PII Protection**: Implement column masking for sensitive data
# MAGIC - [ ] **Data Tagging**: Tag all tables with domain, sensitivity, PII status
# MAGIC - [ ] **Documentation**: Add comments to all tables and key columns
# MAGIC - [ ] **Lineage**: Enable and review data lineage regularly
# MAGIC - [ ] **Audit**: Monitor access patterns via system tables
# MAGIC - [ ] **Data Quality**: Add constraints for critical data rules
# MAGIC - [ ] **Retention**: Implement VACUUM policies and GDPR procedures
# MAGIC - [ ] **Discovery**: Maintain searchable metadata catalog