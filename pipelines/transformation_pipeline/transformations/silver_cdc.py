from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

from utilities.helpers import bronze_table

# ---------------------------------------------------------------------------
# customers — SCD Type 2, report-only business rules
# ---------------------------------------------------------------------------

CUSTOMERS_RULES = {
    "valid_loyalty_tier": "loyalty_tier IN ('bronze','silver','gold')",
    "registration_not_future": "registration_date <= current_timestamp()",
}


@dp.view
def customers_change_feed():
    return (
        spark.readStream.table(bronze_table("bronze_customers_valid"))
        .filter(col("after").isNotNull())
        .select("after.*", "op", "ts_ms")
        .withColumn("registration_date", col("registration_date").cast("timestamp"))
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )


dp.create_streaming_table(
    name="silver_customers",
    comment="Customers, SCD Type 2 — full history of profile and loyalty-tier changes.",
    expect_all=CUSTOMERS_RULES,
)

dp.create_auto_cdc_flow(
    target="silver_customers",
    source="customers_change_feed",
    keys=["customer_id"],
    sequence_by="ts_ms",
    stored_as_scd_type=2,
    ignore_null_updates=True,
    except_column_list= ["op"]
)


