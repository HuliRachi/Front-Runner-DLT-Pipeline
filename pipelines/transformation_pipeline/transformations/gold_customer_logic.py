from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, max as _max, datediff, lit, to_date


def compute_customer_360(
    customers_df: DataFrame,
    clickstream_df: DataFrame,
    as_of_date: date,
) -> DataFrame:
    # Filter SCD Type 2 fields to catch only the active customer state records
    customers_current = customers_df.filter(col("__END_AT").isNull())

    # Isolate conversion events tied to explicit order actions from telemetry data
    purchase_summary = (
        clickstream_df
        .filter(col("event_type") == "purchase")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("purchase_count"),
            _max("event_timestamp").alias("last_purchase_date"),
        )
    )

    return (
        customers_current
        .join(purchase_summary, "customer_id", "left")
        .withColumn(
            "days_since_last_purchase",
            datediff(lit(as_of_date), to_date(col("last_purchase_date"))),
        )
        .select(
            "customer_id", "first_name", "last_name", "loyalty_tier",
            "country", "purchase_count", "last_purchase_date", "days_since_last_purchase",
        )
    )
