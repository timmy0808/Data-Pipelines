"""
Silver-layer customer transformations.

Creates:
    silver_customers
        Validated, standardized, and deduplicated customer records.

    quarantine_customers
        Invalid customer records retained for investigation.

Source:
    bronze_customers
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALID_CUSTOMER_CONDITION = """
    customer_id IS NOT NULL
    AND trim(customer_id) <> ''
    AND email IS NOT NULL
    AND email LIKE '%@%'
    AND updated_at IS NOT NULL
"""


def standardize_customers(df: DataFrame) -> DataFrame:
    """
    Standardize Bronze customer records and assign validation errors.
    """

    return (
        df.select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.initcap(F.trim(F.col("first_name"))).alias("first_name"),
            F.initcap(F.trim(F.col("last_name"))).alias("last_name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.initcap(F.trim(F.col("city"))).alias("city"),
            F.upper(F.trim(F.col("state"))).alias("state"),
            F.initcap(F.trim(F.col("region"))).alias("region"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("_rescued_data"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "_validation_error",
            F.when(
                F.col("customer_id").isNull()
                | (F.length(F.col("customer_id")) == 0),
                F.lit("missing_customer_id"),
            )
            .when(
                F.col("email").isNull()
                | (F.length(F.col("email")) == 0),
                F.lit("missing_email"),
            )
            .when(
                ~F.col("email").contains("@"),
                F.lit("invalid_email"),
            )
            .when(
                F.col("updated_at").isNull(),
                F.lit("missing_or_invalid_updated_at"),
            )
            .otherwise(F.lit(None)),
        )
    )


@dp.temporary_view(
    name="customers_standardized",
    comment="Standardized customer records before validation.",
)
def customers_standardized() -> DataFrame:
    """
    Read Bronze customer data and apply standardization rules.
    """

    return standardize_customers(
        spark.readStream.table("bronze_customers")
    )


@dp.table(
    name="silver_customers",
    comment=(
        "Validated, standardized, and deduplicated retail customers."
    ),
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dp.expect_or_drop(
    "valid_customer",
    VALID_CUSTOMER_CONDITION,
)
def silver_customers() -> DataFrame:
    """
    Produce the clean Silver customer table.

    The expectation references columns returned by this function rather
    than a temporary `_is_valid` column.
    """

    return (
        spark.readStream.table("customers_standardized")
        .filter(F.col("_validation_error").isNull())
        .withWatermark("updated_at", "30 days")
        .dropDuplicates(["customer_id"])
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "state",
            "region",
            "created_at",
            "updated_at",
            "_source_file",
            "_ingested_at",
        )
    )


@dp.table(
    name="quarantine_customers",
    comment=(
        "Customer records rejected from the Silver layer because they "
        "failed validation."
    ),
    table_properties={
        "quality": "quarantine",
        "layer": "silver",
    },
)
def quarantine_customers() -> DataFrame:
    """
    Preserve invalid customer records for review and reprocessing.
    """

    return (
        spark.readStream.table("customers_standardized")
        .filter(F.col("_validation_error").isNotNull())
        .withColumn(
            "_quarantined_at",
            F.current_timestamp(),
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "state",
            "region",
            "created_at",
            "updated_at",
            "_validation_error",
            "_rescued_data",
            "_source_file",
            "_ingested_at",
            "_quarantined_at",
        )
    )