from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, coalesce, lit


def compute_product_performance(
    products_df: DataFrame,
    clickstream_df: DataFrame,
) -> DataFrame:
    # Isolate product discovery views since product_id is explicitly present here
    view_summary = (
        clickstream_df
        .filter(col("event_type") == "product_view")
        .groupBy("product_id")
        .agg(count("event_id").alias("view_count"))
    )

    return (
        products_df
        .join(view_summary, "product_id", "left")
        .withColumn("view_count", coalesce(col("view_count"), lit(0)))
        .select(
            "product_id", "sku", "name", "category", 
            "price_usd", "weight_kg", "view_count"
        )
    )
