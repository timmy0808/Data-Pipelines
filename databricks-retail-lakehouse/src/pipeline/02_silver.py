"""
Silver-layer product transformations.

Creates:
    silver_products
        Validated, standardized, and deduplicated products.

    quarantine_products
        Invalid products retained for investigation.

Source:
    bronze_products
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


VALID_PRODUCT_CONDITION = """
    product_id IS NOT NULL
    AND trim(product_id) <> ''
    AND product_name IS NOT NULL
    AND trim(product_name) <> ''
    AND unit_price IS NOT NULL
    AND unit_price >= 0
"""


def standardize_products(df: DataFrame) -> DataFrame:
    """
    Standardize product records and assign validation errors.
    """

    normalized_active = (
        F.when(
            F.lower(F.trim(F.col("active").cast("string"))).isin(
                "true",
                "1",
                "yes",
                "y",
                "active",
            ),
            F.lit(True),
        )
        .when(
            F.lower(F.trim(F.col("active").cast("string"))).isin(
                "false",
                "0",
                "no",
                "n",
                "inactive",
            ),
            F.lit(False),
        )
        .otherwise(F.lit(None).cast("boolean"))
    )

    return (
        df.select(
            F.trim(F.col("product_id")).alias("product_id"),
            F.initcap(F.trim(F.col("product_name"))).alias(
                "product_name"
            ),
            F.initcap(F.trim(F.col("category"))).alias("category"),
            F.initcap(F.trim(F.col("subcategory"))).alias(
                "subcategory"
            ),
            F.round(
                F.col("unit_price").cast("decimal(18, 2)"),
                2,
            ).alias("unit_price"),
            normalized_active.alias("active"),
            F.col("created_at").cast("timestamp").alias("created_at"),
            F.col("updated_at").cast("timestamp").alias("updated_at"),
            F.col("_rescued_data"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "_validation_error",
            F.when(
                F.col("product_id").isNull()
                | (F.length(F.col("product_id")) == 0),
                F.lit("missing_product_id"),
            )
            .when(
                F.col("product_name").isNull()
                | (F.length(F.col("product_name")) == 0),
                F.lit("missing_product_name"),
            )
            .when(
                F.col("unit_price").isNull(),
                F.lit("missing_or_invalid_unit_price"),
            )
            .when(
                F.col("unit_price") < 0,
                F.lit("negative_unit_price"),
            )
            .when(
                F.col("active").isNull(),
                F.lit("invalid_active_value"),
            )
            .otherwise(F.lit(None)),
        )
    )


@dp.temporary_view(
    name="products_standardized",
    comment="Standardized product records before validation.",
)
def products_standardized() -> DataFrame:
    return standardize_products(
        spark.readStream.table("bronze_products")
    )


@dp.table(
    name="silver_products",
    comment="Validated and standardized retail products.",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dp.expect_or_drop(
    "valid_product",
    VALID_PRODUCT_CONDITION,
)
def silver_products() -> DataFrame:
    return (
        spark.readStream.table("products_standardized")
        .filter(F.col("_validation_error").isNull())
        .dropDuplicates(["product_id"])
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "unit_price",
            "active",
            "created_at",
            "updated_at",
            "_source_file",
            "_ingested_at",
        )
    )


@dp.table(
    name="quarantine_products",
    comment="Product records rejected during Silver validation.",
    table_properties={
        "quality": "quarantine",
        "layer": "silver",
    },
)
def quarantine_products() -> DataFrame:
    return (
        spark.readStream.table("products_standardized")
        .filter(F.col("_validation_error").isNotNull())
        .withColumn("_quarantined_at", F.current_timestamp())
        .select(
            "product_id",
            "product_name",
            "category",
            "subcategory",
            "unit_price",
            "active",
            "created_at",
            "updated_at",
            "_validation_error",
            "_rescued_data",
            "_source_file",
            "_ingested_at",
            "_quarantined_at",
        )
    )