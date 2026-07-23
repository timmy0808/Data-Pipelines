from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SOURCE_PATH = spark.conf.get("retail.source_path")

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("region", StringType()),
    StructField("created_at", TimestampType()),
    StructField("updated_at", TimestampType()),
    StructField("_rescued_data", StringType()),
])

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("active", BooleanType()),
    StructField("updated_at", TimestampType()),
    StructField("_rescued_data", StringType()),
])

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_timestamp", TimestampType()),
    StructField("status", StringType()),
    StructField("payment_method", StringType()),
    StructField("order_total", DoubleType()),
    StructField("_rescued_data", StringType()),
])

order_item_schema = StructType([
    StructField("order_item_id", StringType()),
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("line_total", DoubleType()),
    StructField("_rescued_data", StringType()),
])


def autoloader_json(entity: str, schema: StructType):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .load(f"{SOURCE_PATH}/{entity}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_customers",
    comment="Raw customer records incrementally ingested with Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return autoloader_json("customers", customer_schema)


@dp.table(
    name="bronze_products",
    comment="Raw product master records incrementally ingested with Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return autoloader_json("products", product_schema)


@dp.table(
    name="bronze_orders",
    comment="Raw retail orders incrementally ingested with Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return autoloader_json("orders", order_schema)


@dp.table(
    name="bronze_order_items",
    comment="Raw order-item records incrementally ingested with Auto Loader.",
    table_properties={"quality": "bronze"},
)
def bronze_order_items():
    return autoloader_json("order_items", order_item_schema)
