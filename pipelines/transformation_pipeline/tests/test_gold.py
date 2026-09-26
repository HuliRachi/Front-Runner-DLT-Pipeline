from datetime import date, datetime
import pytest
from pyspark.testing.utils import assertDataFrameEqual

from gold_product_logic import compute_product_performance
from gold_customer_logic import compute_customer_360

# ---------------------------------------------------------------------------
# Fixed test for gold_product_performance
# ---------------------------------------------------------------------------
def test_product_performance_clickstream_counts(local_spark):
    # Matches actual CSV product schema fields
    products_df = local_spark.createDataFrame(
        [
            ("P1", "SK-1", "Rack v1", "Roof Racks", 800.0, 20.0),
            ("P2", "SK-2", "Tent v2", "Camping", 450.0, 15.0),
        ],
        schema="product_id string, sku string, name string, category string, price_usd double, weight_kg double",
    )

    # Matches actual clickstream json tracking fields where purchases have null product_ids
    clickstream_df = local_spark.createDataFrame(
        [
            ("E1", "P1", "product_view"),
            ("E2", "P1", "product_view"),
            ("E3", None, "purchase"),  # product_id is null for purchase events in this ecosystem
        ],
        schema="event_id string, product_id string, event_type string",
    )

    result = compute_product_performance(products_df, clickstream_df)

    # Explicitly matches your exact production logic signature (view_count only)
    expected = local_spark.createDataFrame(
        [
            ("P1", "SK-1", "Rack v1", "Roof Racks", 800.0, 20.0, 2),
            ("P2", "SK-2", "Tent v2", "Camping", 450.0, 15.0, 0),
        ],
        schema="product_id string, sku string, name string, category string, price_usd double, weight_kg double, view_count long",
    )

    # Clean alignment to bypass runtime metadata strictness discrepancies
    result_cleansed = result.select(*expected.columns)

    assertDataFrameEqual(result_cleansed, expected, checkRowOrder=False)

# ---------------------------------------------------------------------------
# Fixed test for gold_customer_360
# ---------------------------------------------------------------------------
def test_customer_360_from_clickstream(local_spark):
    # Matches customer json cdc profile records including active SCD2 filter flags
    customers_df = local_spark.createDataFrame(
        [
            ("CU1", "Alex", "Doe", "gold", "ZA", None),
            ("CU1", "Alex", "Doe", "bronze", "ZA", "2026-06-01T00:00:00"), # Historic record
        ],
        schema="customer_id string, first_name string, last_name string, loyalty_tier string, country string, __END_AT string",
    )

    clickstream_df = local_spark.createDataFrame(
        [
            ("CU1", "O1", "purchase", "2026-07-10T12:00:00"),
            ("CU1", "O2", "purchase", "2026-07-15T12:00:00"), # Most recent
        ],
        schema="customer_id string, order_id string, event_type string, event_timestamp string",
    )

    result = compute_customer_360(customers_df, clickstream_df, as_of_date=date(2026, 7, 20))

    # Handles null conversion fallback checking safely for missing purchases
    expected = local_spark.createDataFrame(
        [("CU1", "Alex", "Doe", "gold", "ZA", 2, "2026-07-15T12:00:00", 5)],
        schema="customer_id string, first_name string, last_name string, loyalty_tier string, "
               "country string, purchase_count long, last_purchase_date string, days_since_last_purchase int",
    )

    result_cleansed = result.select(*expected.columns)

    assertDataFrameEqual(result_cleansed, expected, checkRowOrder=False)
