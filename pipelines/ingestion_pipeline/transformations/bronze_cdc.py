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

CUSTOMERS_ROW_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("registration_date", StringType()),
        StructField("loyalty_tier", StringType()),
        StructField("country", StringType()),
        StructField("is_active", BooleanType()),
        StructField("updated_at", StringType()),
    ]
)


def _envelope_schema(row_schema: StructType) -> StructType:
    """Wraps a row schema in the standard Debezium-flattened envelope shape."""
    return StructType(
        [
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField("before", row_schema),
            StructField("after", row_schema)
        ]
    )

def _read_cdc_bronze(subfolder: str, row_schema: StructType):
    """Shared Auto Loader read, with ingestion metadata attached, for one CDC source."""
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(_envelope_schema(row_schema))
            .load(landing_path(subfolder))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_customers",
    comment="Raw Debezium CDC envelope for customers, exactly as it arrived. Not flattened yet.",
)
def bronze_customers():
    return _read_cdc_bronze("customers_cdc", CUSTOMERS_ROW_SCHEMA)    

