from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)

from utilities.helpers import get_catalog, landing_path

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("sku", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("weight_kg", DoubleType()),
        StructField("price_usd", DoubleType()),
    ]
)


CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("product_id", StringType()),
        StructField("page_url", StringType()),
        StructField("referrer", StringType()),
        StructField("device_type", StringType()),
        StructField("order_id", StringType()),
    ]
)


def _read_file_bronze(subfolder: str, schema: StructType, file_format: str):
    """Shared Auto Loader read, with ingestion metadata attached, for one file source."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .schema(schema)
    )
    if file_format == "csv":
        reader = reader.option("header", "true")

    return (
        reader.load(landing_path(subfolder))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_products",
    comment="Raw product catalog export, exactly as it arrived.",
)
def bronze_products():
    return _read_file_bronze("products", PRODUCTS_SCHEMA, "csv")


@dp.table(
    name="bronze_clickstream",
    comment="Raw storefront clickstream events, append-only, exactly as they arrived.",
)
def bronze_clickstream():
    return _read_file_bronze("clickstream", CLICKSTREAM_SCHEMA, "json")

