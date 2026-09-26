import sys
from datetime import datetime, timezone
 
def get_arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for arg in sys.argv[1:]:
        if arg.startswith(prefix):
            return arg[len(prefix):]
    return default
 
 
catalog = get_arg("catalog", "dev")
run_date = get_arg("run_date", datetime.now(timezone.utc).date().isoformat())
 
BRONZE_TABLES = [
    "bronze_customers", "bronze_products", "bronze_clickstream"
]

SILVER_TABLES = [
    "silver_customers", "silver_products", "silver_clickstream"
]

GOLD_TABLES = [
    "gold_customer_360", "gold_product_performance"
]

DATE_COLUMNS = {
    "bronze_customers": ("_ingested_at", "timestamp"),
    "bronze_products": ("_ingested_at", "timestamp"),
    "bronze_clickstream": ("_ingested_at", "timestamp"),
}
 
BRONZE_QUARANTINE_PAIRS = [
    ("bronze_customers_valid", "bronze_customers_quarantined", "_ingested_at", "timestamp"),
    ("bronze_products_valid", "bronze_products_quarantined", "_ingested_at", "timestamp"),
    ("bronze_clickstream_valid", "bronze_clickstream_quarantined", "_ingested_at", "timestamp"),
]
 

SILVER_QUARANTINE_PAIRS = [
    ("silver_customers", "silver_customers_quarantined"),
    ("silver_products", "silver_products_quarantined"),
    ("silver_clickstream", "silver_clickstream_quarantined"),
]
 
 
def date_filter(date_column: str, date_column_kind: str) -> str:
    if date_column_kind == "epoch_ms":
        return f"date(from_unixtime({date_column} / 1000)) = '{run_date}'"
    return f"date({date_column}) = '{run_date}'"
 
 
def count_table(name: str, today_only: bool = False) -> int:
    try:
        df = spark.table(f"{catalog}.frontrunner.{name}")
        if today_only and name in DATE_COLUMNS:
            col, kind = DATE_COLUMNS[name]
            df = df.filter(date_filter(col, kind))
        return df.count()
    except Exception:
        return -1  
 
 
def fmt(count: int) -> str:
    return str(count) if count >= 0 else "ERROR"
 
 
def print_section(title: str, tables: list[str], show_today: bool) -> None:
    print(f"\n{title}:")
    for t in tables:
        total = count_table(t)
        if show_today and t in DATE_COLUMNS:
            today = count_table(t, today_only=True)
            print(f"  {t:<32} total={fmt(total):<8} today={fmt(today)}")
        else:
            print(f"  {t:<32} total={fmt(total)}")
 
 
print(f"=== frontrunner Run Summary — catalog={catalog}, run_date={run_date} ===")
 
print_section("Bronze", BRONZE_TABLES, show_today=True)
print_section("Silver", SILVER_TABLES, show_today=False) 
print_section("Gold", GOLD_TABLES, show_today=False)  
 
print("\nQuarantine — Bronze (scoped to run_date):")
for valid_table, quarantined_table, date_column, date_kind in BRONZE_QUARANTINE_PAIRS:
    filter_expr = date_filter(date_column, date_kind)
    try:
        valid_today = spark.table(f"{catalog}.frontrunner.{valid_table}").filter(filter_expr).count()
        quarantined_today = spark.table(f"{catalog}.frontrunner.{quarantined_table}").filter(filter_expr).count()
        total_today = valid_today + quarantined_today
        rate = (quarantined_today / total_today) if total_today > 0 else 0.0
        print(f"  {quarantined_table:<32} {quarantined_today} quarantined of {total_today} today ({rate:.1%})")
    except Exception:
        print(f"  {quarantined_table:<32} ERROR")
 
print("\nQuarantine — Silver (total, not date-scoped — no processing-time column):")
for valid_table, quarantined_table in SILVER_QUARANTINE_PAIRS:
    try:
        valid_total = spark.table(f"{catalog}.frontrunner.{valid_table}").count()
        quarantined_total = spark.table(f"{catalog}.frontrunner.{quarantined_table}").count()
        total = valid_total + quarantined_total
        rate = (quarantined_total / total) if total > 0 else 0.0
        print(f"  {quarantined_table:<32} {quarantined_total} quarantined of {total} total ({rate:.1%})")
    except Exception:
        print(f"  {quarantined_table:<32} ERROR")

print("\nNote: this is a lightweight per-run summary. Full DQ dashboarding, trend "
      "lines, and event-hook alerting are built in later Module")
