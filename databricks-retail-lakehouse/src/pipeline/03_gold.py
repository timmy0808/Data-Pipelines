from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="dim_customer",
    comment="Current customer dimension containing the latest record per customer.",
    table_properties={"quality": "gold"},
)
def dim_customer():
    window = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
    return (
        spark.read.table("silver_customers")
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .select(
            "customer_id",
            F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
            "email",
            "city",
            "state",
            "region",
            "created_at",
            "updated_at",
        )
    )


@dp.materialized_view(
    name="dim_product",
    comment="Current product dimension containing the latest record per product.",
    table_properties={"quality": "gold"},
)
def dim_product():
    window = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc())
    return (
        spark.read.table("silver_products")
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .select(
            "product_id",
            "product_name",
            "category",
            "unit_price",
            "active",
            "updated_at",
        )
    )


@dp.materialized_view(
    name="fact_orders",
    comment="Conformed order fact joined to the customer dimension.",
    table_properties={"quality": "gold"},
)
def fact_orders():
    orders = spark.read.table("silver_orders").alias("o")
    customers = spark.read.table("dim_customer").alias("c")
    return (
        orders.join(customers, "customer_id", "left")
        .select(
            "order_id",
            "customer_id",
            F.col("c.full_name").alias("customer_name"),
            F.col("c.region").alias("customer_region"),
            "order_timestamp",
            "order_date",
            "status",
            "payment_method",
            "order_total",
        )
    )


@dp.materialized_view(
    name="fact_order_items",
    comment="Order-item fact enriched with product attributes.",
    table_properties={"quality": "gold"},
)
def fact_order_items():
    items = spark.read.table("silver_order_items").alias("i")
    products = spark.read.table("dim_product").alias("p")
    orders = spark.read.table("silver_orders").alias("o")
    return (
        items
        .join(products, "product_id", "left")
        .join(orders.select("order_id", "customer_id", "order_date", "status"), "order_id", "left")
        .select(
            "order_item_id",
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "product_id",
            "product_name",
            "category",
            "quantity",
            F.col("i.unit_price").alias("sale_unit_price"),
            "line_total",
        )
    )


@dp.materialized_view(
    name="gold_daily_sales",
    comment="Daily retail KPIs excluding cancelled orders.",
    table_properties={"quality": "gold"},
)
def gold_daily_sales():
    return (
        spark.read.table("fact_orders")
        .filter(F.col("status") != "CANCELLED")
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.round(F.sum("order_total"), 2).alias("revenue"),
            F.round(F.avg("order_total"), 2).alias("average_order_value"),
        )
    )


@dp.materialized_view(
    name="gold_customer_metrics",
    comment="Customer order counts and lifetime-value metrics.",
    table_properties={"quality": "gold"},
)
def gold_customer_metrics():
    orders = spark.read.table("fact_orders").filter(F.col("status") != "CANCELLED")
    customers = spark.read.table("dim_customer")
    metrics = (
        orders.groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("order_total"), 2).alias("lifetime_value"),
            F.max("order_date").alias("most_recent_order_date"),
        )
    )
    return (
        customers.join(metrics, "customer_id", "left")
        .fillna({"order_count": 0, "lifetime_value": 0.0})
        .select(
            "customer_id",
            "full_name",
            "region",
            "order_count",
            "lifetime_value",
            "most_recent_order_date",
        )
    )


@dp.materialized_view(
    name="gold_product_performance",
    comment="Product sales performance excluding cancelled orders.",
    table_properties={"quality": "gold"},
)
def gold_product_performance():
    return (
        spark.read.table("fact_order_items")
        .filter(F.col("status") != "CANCELLED")
        .groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("line_total"), 2).alias("revenue"),
        )
    )
