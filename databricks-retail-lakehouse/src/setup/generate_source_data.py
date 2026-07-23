# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Retail Source Data
# MAGIC
# MAGIC This notebook creates a catalog, schemas, a Unity Catalog volume, and two
# MAGIC incremental batches of JSON source records.

# COMMAND ----------

import json
from datetime import datetime

dbutils.widgets.text("catalog", "retail_dev")
dbutils.widgets.text("source_schema", "retail_sources")
dbutils.widgets.text("volume", "landing")

catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{source_schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{source_schema}`.`{volume}`")

root = f"/Volumes/{catalog}/{source_schema}/{volume}"

# Remove old demo input so repeated portfolio runs remain deterministic.
for entity in ["customers", "products", "orders", "order_items"]:
    path = f"{root}/{entity}"
    try:
        dbutils.fs.rm(path, recurse=True)
    except Exception:
        pass
    dbutils.fs.mkdirs(path)

customers_batch_1 = [
    {
        "customer_id": "C001",
        "first_name": "Ava",
        "last_name": "Martinez",
        "email": "ava.martinez@example.com",
        "city": "Denver",
        "state": "CO",
        "region": "West",
        "created_at": "2026-01-03T10:15:00Z",
        "updated_at": "2026-01-03T10:15:00Z"
    },
    {
        "customer_id": "C002",
        "first_name": "Noah",
        "last_name": "Williams",
        "email": "noah.williams@example.com",
        "city": "Austin",
        "state": "TX",
        "region": "South",
        "created_at": "2026-01-05T09:00:00Z",
        "updated_at": "2026-01-05T09:00:00Z"
    },
    {
        "customer_id": "C003",
        "first_name": "Mia",
        "last_name": "Chen",
        "email": "mia.chen@example.com",
        "city": "Seattle",
        "state": "WA",
        "region": "West",
        "created_at": "2026-01-06T13:45:00Z",
        "updated_at": "2026-01-06T13:45:00Z"
    },
    {
        "customer_id": None,
        "first_name": "Invalid",
        "last_name": "Customer",
        "email": "not-an-email",
        "city": "Unknown",
        "state": "XX",
        "region": "Unknown",
        "created_at": "2026-01-06T13:45:00Z",
        "updated_at": "2026-01-06T13:45:00Z"
    }
]

customers_batch_2 = [
    {
        "customer_id": "C002",
        "first_name": "Noah",
        "last_name": "Williams",
        "email": "noah.williams@example.com",
        "city": "Dallas",
        "state": "TX",
        "region": "South",
        "created_at": "2026-01-05T09:00:00Z",
        "updated_at": "2026-02-10T11:30:00Z"
    },
    {
        "customer_id": "C004",
        "first_name": "Liam",
        "last_name": "Johnson",
        "email": "liam.johnson@example.com",
        "city": "Chicago",
        "state": "IL",
        "region": "Midwest",
        "created_at": "2026-02-01T08:20:00Z",
        "updated_at": "2026-02-01T08:20:00Z"
    },
    {
        "customer_id": "C005",
        "first_name": "Sophia",
        "last_name": "Brown",
        "email": "sophia.brown@example.com",
        "city": "Boston",
        "state": "MA",
        "region": "Northeast",
        "created_at": "2026-02-04T16:10:00Z",
        "updated_at": "2026-02-04T16:10:00Z"
    }
]

products_batch_1 = [
    {"product_id": "P100", "product_name": "Trail Running Shoes", "category": "Footwear", "unit_price": 129.99, "active": True, "updated_at": "2026-01-01T00:00:00Z"},
    {"product_id": "P101", "product_name": "Insulated Water Bottle", "category": "Accessories", "unit_price": 34.50, "active": True, "updated_at": "2026-01-01T00:00:00Z"},
    {"product_id": "P102", "product_name": "Climbing Chalk Bag", "category": "Climbing", "unit_price": 29.95, "active": True, "updated_at": "2026-01-01T00:00:00Z"},
    {"product_id": "P103", "product_name": "Lightweight Rain Jacket", "category": "Outerwear", "unit_price": 179.00, "active": True, "updated_at": "2026-01-01T00:00:00Z"}
]

products_batch_2 = [
    {"product_id": "P104", "product_name": "Camping Headlamp", "category": "Accessories", "unit_price": 49.99, "active": True, "updated_at": "2026-02-01T00:00:00Z"},
    {"product_id": "P105", "product_name": "Approach Backpack", "category": "Packs", "unit_price": 109.00, "active": True, "updated_at": "2026-02-01T00:00:00Z"}
]

orders_batch_1 = [
    {"order_id": "O1001", "customer_id": "C001", "order_timestamp": "2026-01-10T14:10:00Z", "status": "SHIPPED", "payment_method": "CARD", "order_total": 164.49},
    {"order_id": "O1002", "customer_id": "C002", "order_timestamp": "2026-01-12T09:40:00Z", "status": "DELIVERED", "payment_method": "PAYPAL", "order_total": 209.95},
    {"order_id": "O1003", "customer_id": "C003", "order_timestamp": "2026-01-15T17:25:00Z", "status": "CANCELLED", "payment_method": "CARD", "order_total": 29.95},
    {"order_id": "BAD001", "customer_id": None, "order_timestamp": "2026-01-16T10:00:00Z", "status": "UNKNOWN", "payment_method": "CARD", "order_total": -50.00}
]

orders_batch_2 = [
    {"order_id": "O1004", "customer_id": "C002", "order_timestamp": "2026-02-11T11:00:00Z", "status": "SHIPPED", "payment_method": "CARD", "order_total": 49.99},
    {"order_id": "O1005", "customer_id": "C004", "order_timestamp": "2026-02-12T15:15:00Z", "status": "PROCESSING", "payment_method": "CARD", "order_total": 138.95},
    {"order_id": "O1006", "customer_id": "C005", "order_timestamp": "2026-01-20T08:30:00Z", "status": "DELIVERED", "payment_method": "PAYPAL", "order_total": 179.00},
    {"order_id": "O1004", "customer_id": "C002", "order_timestamp": "2026-02-11T11:00:00Z", "status": "SHIPPED", "payment_method": "CARD", "order_total": 49.99}
]

items_batch_1 = [
    {"order_item_id": "OI001", "order_id": "O1001", "product_id": "P100", "quantity": 1, "unit_price": 129.99, "line_total": 129.99},
    {"order_item_id": "OI002", "order_id": "O1001", "product_id": "P101", "quantity": 1, "unit_price": 34.50, "line_total": 34.50},
    {"order_item_id": "OI003", "order_id": "O1002", "product_id": "P103", "quantity": 1, "unit_price": 179.00, "line_total": 179.00},
    {"order_item_id": "OI004", "order_id": "O1002", "product_id": "P102", "quantity": 1, "unit_price": 29.95, "line_total": 29.95},
    {"order_item_id": "OI005", "order_id": "O1003", "product_id": "P102", "quantity": 1, "unit_price": 29.95, "line_total": 29.95}
]

items_batch_2 = [
    {"order_item_id": "OI006", "order_id": "O1004", "product_id": "P104", "quantity": 1, "unit_price": 49.99, "line_total": 49.99},
    {"order_item_id": "OI007", "order_id": "O1005", "product_id": "P105", "quantity": 1, "unit_price": 109.00, "line_total": 109.00},
    {"order_item_id": "OI008", "order_id": "O1005", "product_id": "P102", "quantity": 1, "unit_price": 29.95, "line_total": 29.95},
    {"order_item_id": "OI009", "order_id": "O1006", "product_id": "P103", "quantity": 1, "unit_price": 179.00, "line_total": 179.00}
]

def put_json_lines(path: str, records: list[dict]) -> None:
    content = "\n".join(json.dumps(record) for record in records)
    dbutils.fs.put(path, content, overwrite=True)

datasets = {
    "customers": [customers_batch_1, customers_batch_2],
    "products": [products_batch_1, products_batch_2],
    "orders": [orders_batch_1, orders_batch_2],
    "order_items": [items_batch_1, items_batch_2],
}

for entity, batches in datasets.items():
    for number, records in enumerate(batches, start=1):
        put_json_lines(
            f"{root}/{entity}/batch_{number:03d}.json",
            records
        )

print(f"Generated source files under {root}")
display(dbutils.fs.ls(root))
