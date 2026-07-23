"""
Silver-layer transformations for retail orders.

Creates:
    silver_orders
        Validated, standardized, and deduplicated order records.

    quarantine_orders
        Invalid order records retained for investigation.

Source:
    bronze_orders
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALID_ORDER_STATUSES = [
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
]

VALID_ORDER_CONDITION = """
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND order_timestamp IS NOT NULL
    AND status IN (
        'PROCESSING',
        'SHIPPED',
        'DELIVERED',
        'CANCELLED'
    )
    AND order_total IS NOT NULL
    AND order_total >= 0
"""


def standardize_orders(df: DataFrame) -> DataFrame:
    """
    Standardize Bronze order records before validation.

    Transformations:
        - Trim identifier fields
        - Normalize status and payment method to uppercase
        - Round monetary values to two decimal places
        - Derive order_date
        - Add a validation failure reason
    """

    normalized_status = F.upper(F.trim(F.col("status")))

    return (
        df.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.col("order_timestamp").cast("timestamp").alias(
                "order_timestamp"
            ),
            normalized_status.alias("status"),
            F.upper(F.trim(F.col("payment_method"))).alias(
                "payment_method"
            ),
            F.round(
                F.col("order_total").cast("decimal(18, 2)"),
                2,
            ).alias("order_total"),
            F.col("_rescued_data"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "order_date",
            F.to_date(F.col("order_timestamp")),
        )
        .withColumn(
            "_validation_error",
            F.when(
                F.col("order_id").isNull()
                | (F.length(F.col("order_id")) == 0),
                F.lit("missing_order_id"),
            )
            .when(
                F.col("customer_id").isNull()
                | (F.length(F.col("customer_id")) == 0),
                F.lit("missing_customer_id"),
            )
            .when(
                F.col("order_timestamp").isNull(),
                F.lit("missing_or_invalid_order_timestamp"),
            )
            .when(
                ~F.col("status").isin(VALID_ORDER_STATUSES),
                F.lit("invalid_order_status"),
            )
            .when(
                F.col("order_total").isNull(),
                F.lit("missing_or_invalid_order_total"),
            )
            .when(
                F.col("order_total") < 0,
                F.lit("negative_order_total"),
            )
            .otherwise(F.lit(None)),
        )
    )


@dp.temporary_view(
    name="orders_standardized",
    comment="Standardized Bronze order records before validation.",
)
def orders_standardized() -> DataFrame:
    """
    Read and standardize incoming Bronze order records.
    """

    return standardize_orders(
        spark.readStream.table("bronze_orders")
    )


@dp.table(
    name="silver_orders",
    comment=(
        "Validated, standardized, and deduplicated retail orders."
    ),
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dp.expect_or_drop(
    "valid_order",
    VALID_ORDER_CONDITION,
)
def silver_orders() -> DataFrame:
    """
    Produce the clean Silver orders streaming table.

    The expectation references only columns present in the returned
    DataFrame, avoiding the unresolved `_is_valid` column error.
    """

    return (
        spark.readStream.table("orders_standardized")
        .filter(F.col("_validation_error").isNull())
        .withWatermark("order_timestamp", "60 days")
        .dropDuplicates(["order_id"])
        .select(
            "order_id",
            "customer_id",
            "order_timestamp",
            "order_date",
            "status",
            "payment_method",
            "order_total",
            "_source_file",
            "_ingested_at",
        )
    )


@dp.table(
    name="quarantine_orders",
    comment=(
        "Order records rejected from the Silver layer because they "
        "failed data-quality validation."
    ),
    table_properties={
        "quality": "quarantine",
        "layer": "silver",
    },
)
def quarantine_orders() -> DataFrame:
    """
    Preserve invalid records for investigation and reprocessing.
    """

    return (
        spark.readStream.table("orders_standardized")
        .filter(F.col("_validation_error").isNotNull())
        .withColumn(
            "_quarantined_at",
            F.current_timestamp(),
        )
        .select(
            "order_id",
            "customer_id",
            "order_timestamp",
            "order_date",
            "status",
            "payment_method",
            "order_total",
            "_validation_error",
            "_rescued_data",
            "_source_file",
            "_ingested_at",
            "_quarantined_at",
        )
    )