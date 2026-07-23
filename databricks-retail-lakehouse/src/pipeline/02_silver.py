from pyspark import pipelines as dp
from pyspark.sql import functions as F


VALID_ORDER_STATUSES = ["PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]


@dp.temporary_view(name="customers_classified")
def customers_classified():
    return (
        spark.readStream.table("bronze_customers")
        .withColumn(
            "_is_valid",
            F.col("customer_id").isNotNull()
            & F.col("email").contains("@")
            & F.col("updated_at").isNotNull()
        )
    )


@dp.table(
    name="silver_customers",
    comment="Validated, standardized, and deduplicated customer records.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_customer", "_is_valid = true")
def silver_customers():
    return (
        spark.readStream.table("customers_classified")
        .withWatermark("updated_at", "30 days")
        .dropDuplicates(["customer_id", "updated_at"])
        .select(
            "customer_id",
            F.initcap("first_name").alias("first_name"),
            F.initcap("last_name").alias("last_name"),
            F.lower("email").alias("email"),
            F.initcap("city").alias("city"),
            F.upper("state").alias("state"),
            F.initcap("region").alias("region"),
            "created_at",
            "updated_at",
            "_source_file",
            "_ingested_at",
        )
    )


@dp.table(
    name="quarantine_customers",
    comment="Customer rows that failed Silver-layer validation.",
    table_properties={"quality": "quarantine"},
)
def quarantine_customers():
    return (
        spark.readStream.table("customers_classified")
        .filter("NOT _is_valid")
        .withColumn(
            "_quarantine_reason",
            F.lit("Missing customer_id, invalid email, or missing updated_at")
        )
    )


@dp.table(
    name="silver_products",
    comment="Validated and standardized product master.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("positive_product_price", "unit_price > 0")
def silver_products():
    return (
        spark.readStream.table("bronze_products")
        .withWatermark("updated_at", "30 days")
        .dropDuplicates(["product_id", "updated_at"])
        .select(
            "product_id",
            F.trim("product_name").alias("product_name"),
            F.initcap("category").alias("category"),
            F.round("unit_price", 2).alias("unit_price"),
            "active",
            "updated_at",
            "_source_file",
            "_ingested_at",
        )
    )


@dp.temporary_view(name="orders_classified")
def orders_classified():
    return (
        spark.readStream.table("bronze_orders")
        .withColumn(
            "_is_valid",
            F.col("order_id").isNotNull()
            & F.col("customer_id").isNotNull()
            & F.col("order_timestamp").isNotNull()
            & F.upper("status").isin(VALID_ORDER_STATUSES)
            & (F.col("order_total") >= 0)
        )
    )


@dp.table(
    name="silver_orders",
    comment="Validated and deduplicated order headers.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_order", "_is_valid = true")
def silver_orders():
    return (
        spark.readStream.table("orders_classified")
        .withWatermark("order_timestamp", "60 days")
        .dropDuplicates(["order_id"])
        .select(
            "order_id",
            "customer_id",
            "order_timestamp",
            F.to_date("order_timestamp").alias("order_date"),
            F.upper("status").alias("status"),
            F.upper("payment_method").alias("payment_method"),
            F.round("order_total", 2).alias("order_total"),
            "_source_file",
            "_ingested_at",
        )
    )


@dp.table(
    name="quarantine_orders",
    comment="Order rows that failed Silver-layer validation.",
    table_properties={"quality": "quarantine"},
)
def quarantine_orders():
    return (
        spark.readStream.table("orders_classified")
        .filter("NOT _is_valid")
        .withColumn(
            "_quarantine_reason",
            F.lit("Missing key, invalid status, timestamp, or negative order total")
        )
    )


@dp.table(
    name="silver_order_items",
    comment="Validated and deduplicated order line items.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_order_item_id", "order_item_id IS NOT NULL")
@dp.expect_or_drop("valid_order_reference", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_product_reference", "product_id IS NOT NULL")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("valid_line_total", "line_total >= 0")
def silver_order_items():
    return (
        spark.readStream.table("bronze_order_items")
        .dropDuplicates(["order_item_id"])
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            F.round("unit_price", 2).alias("unit_price"),
            F.round("line_total", 2).alias("line_total"),
            "_source_file",
            "_ingested_at",
        )
    )
