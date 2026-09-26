from pyspark import pipelines as dp
from pyspark.sql.functions import expr

from bronze_quality_logic import quarantine_rule


# ---------------------------------------------------------------------------
# bronze_products (file-based) — taught in depth in the lecture
# ---------------------------------------------------------------------------

PRODUCTS_RULES = {
    "valid_product_id": "product_id IS NOT NULL",
    "valid_sku": "sku IS NOT NULL",
    "valid_price_usd": "price_usd IS NULL OR price_usd > 0",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(PRODUCTS_RULES)
def bronze_products_quality_check():
    return spark.readStream.table("bronze_products").withColumn(
        "is_quarantined", expr(quarantine_rule(PRODUCTS_RULES))
    )


@dp.table(comment="Products that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_products_valid():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = false")


@dp.table(comment="Products that failed at least one structural quality check. Published — monitored in L19.")
def bronze_products_quarantined():
    return spark.readStream.table("bronze_products_quality_check").filter("is_quarantined = true")


# ---------------------------------------------------------------------------
# bronze_customers (CDC) — same shape, walked briefly in the lecture
# ---------------------------------------------------------------------------

CUSTOMERS_RULES = {
    "valid_customer_id": "after.customer_id IS NOT NULL",
    "valid_email": "after.email IS NOT NULL",
    "valid_loyalty_tier": "after.loyalty_tier IS NULL OR after.loyalty_tier IN ('bronze', 'silver', 'gold')",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CUSTOMERS_RULES)
def bronze_customers_quality_check():
    return spark.readStream.table("bronze_customers").withColumn(
        "is_quarantined", expr(quarantine_rule(CUSTOMERS_RULES))
    )


@dp.table(comment="Customers that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_customers_valid():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = false")


@dp.table(comment="Customers that failed at least one structural quality check. Published — monitored in L19.")
def bronze_customers_quarantined():
    return spark.readStream.table("bronze_customers_quality_check").filter("is_quarantined = true")


# ---------------------------------------------------------------------------
# bronze_clickstream (file-based) — same shape, walked briefly in the lecture
# ---------------------------------------------------------------------------

CLICKSTREAM_RULES = {
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_event_timestamp": "event_timestamp IS NOT NULL",
}


@dp.table(private=True, partition_cols=["is_quarantined"])
@dp.expect_all(CLICKSTREAM_RULES)
def bronze_clickstream_quality_check():
    return spark.readStream.table("bronze_clickstream").withColumn(
        "is_quarantined", expr(quarantine_rule(CLICKSTREAM_RULES))
    )


@dp.table(comment="Clickstream events that passed every structural quality check. Published — read by Silver from Module 3 onward.")
def bronze_clickstream_valid():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = false")


@dp.table(comment="Clickstream events that failed at least one structural quality check. Published — monitored in L19.")
def bronze_clickstream_quarantined():
    return spark.readStream.table("bronze_clickstream_quality_check").filter("is_quarantined = true")
