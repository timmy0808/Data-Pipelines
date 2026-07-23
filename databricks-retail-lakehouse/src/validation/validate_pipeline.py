# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Retail Lakehouse
# MAGIC
# MAGIC This notebook performs post-pipeline integration checks and fails the job
# MAGIC when an expected output is missing or empty.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_dev")
dbutils.widgets.text("schema", "retail_lakehouse")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
prefix = f"`{catalog}`.`{schema}`"

required_tables = [
    "bronze_customers",
    "bronze_products",
    "bronze_orders",
    "bronze_order_items",
    "silver_customers",
    "silver_products",
    "silver_orders",
    "silver_order_items",
    "quarantine_customers",
    "quarantine_orders",
    "dim_customer",
    "dim_product",
    "fact_orders",
    "fact_order_items",
    "gold_daily_sales",
    "gold_customer_metrics",
    "gold_product_performance",
]

existing = {
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {prefix}").collect()
}
missing = [table for table in required_tables if table not in existing]
assert not missing, f"Missing required tables: {missing}"

non_empty_tables = [
    "silver_customers",
    "silver_products",
    "silver_orders",
    "silver_order_items",
    "gold_daily_sales",
]

for table in non_empty_tables:
    count = spark.table(f"{catalog}.{schema}.{table}").count()
    assert count > 0, f"{table} should not be empty"
    print(f"{table}: {count} rows")

invalid_orders = spark.table(f"{catalog}.{schema}.quarantine_orders").count()
invalid_customers = spark.table(f"{catalog}.{schema}.quarantine_customers").count()

assert invalid_orders >= 1, "Expected at least one quarantined order"
assert invalid_customers >= 1, "Expected at least one quarantined customer"

duplicate_order_count = spark.sql(
    f"""
    SELECT COUNT(*) AS duplicate_groups
    FROM (
      SELECT order_id
      FROM {prefix}.silver_orders
      GROUP BY order_id
      HAVING COUNT(*) > 1
    )
    """
).first()["duplicate_groups"]

assert duplicate_order_count == 0, "silver_orders contains duplicate order IDs"

print("All integration checks passed.")
