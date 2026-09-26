from datetime import timezone, datetime

from pyspark import pipelines as dp

from gold_customer_logic import compute_customer_360


@dp.materialized_view(
    name="gold_customer_360",
    comment="Customer aggregate metrics and purchase frequency computed from silver layer data models.",
)
def gold_customer_360():
    return compute_customer_360(
        customers_df=spark.read.table("silver_customers"),
        clickstream_df=spark.read.table("silver_clickstream"),
        as_of_date=datetime.now(timezone.utc).date(),
    )
