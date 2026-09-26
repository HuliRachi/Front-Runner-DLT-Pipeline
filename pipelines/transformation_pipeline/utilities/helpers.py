from pyspark.sql import SparkSession


def get_catalog() -> str:
    
    spark = SparkSession.getActiveSession()
    return spark.conf.get("frontrunner.catalog", "dev")


def bronze_table(name: str) -> str:
    
    return f"{get_catalog()}.frontrunner.{name}"
