from pyspark import pipelines as dp

from gold_product_logic import compute_product_performance


@dp.materialized_view(
    name="gold_product_performance",
    comment="Product exposure tracking metrics derived entirely from clickstream view metrics.",
)
def gold_product_performance():
    return compute_product_performance(
        products_df=spark.read.table("silver_products"),
        clickstream_df=spark.read.table("silver_clickstream"),
    )
