from pyspark import pipelines as dp
from pyspark.sql.functions import col

from utilities.helpers import bronze_table

# ---------------------------------------------------------------------------
# products — snapshot source, AUTO CDC upsert (SCD Type 1), report-only rule
# ---------------------------------------------------------------------------

PRODUCTS_RULES = {
    "valid_price_cost": "price_usd > weight_kg",
}


@dp.view
def products_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_products_valid"))
    )


dp.create_streaming_table(
    name="silver_products",
    comment="Products, current state only. Upserted by product_id, latest file wins.",
    expect_all=PRODUCTS_RULES,
    schema="""
        product_id STRING NOT NULL,
        sku STRING,
        name STRING,
        category STRING,
        weight_kg DOUBLE,
        price_usd DOUBLE,
        CONSTRAINT pk_silver_products PRIMARY KEY (product_id)
    """,
)

dp.create_auto_cdc_flow(
    target="silver_products",
    source="products_change_feed",
    keys=["product_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
    except_column_list= ["_ingested_at", "_source_file", "is_quarantined"]
)

# ---------------------------------------------------------------------------
# clickstream,  
# ---------------------------------------------------------------------------

CLICKSTREAM_RULES = {
    "purchase_has_order": "NOT (event_type = 'purchase' AND order_id IS NULL)",
    "product_event_has_product": "NOT (event_type IN ('product_view', 'add_to_cart') AND product_id IS NULL)",
}


@dp.table(
    comment="Clickstream events, conformed — real timestamp type. Already an append-only "
    "event log, so no merge logic is needed; every row is naturally distinct.",
)
@dp.expect_all(CLICKSTREAM_RULES)
def silver_clickstream():
    return (
        spark.readStream.table(bronze_table("bronze_clickstream_valid"))
        .withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
        .drop("_ingested_at", "_source_file", "is_quarantined")
    )
