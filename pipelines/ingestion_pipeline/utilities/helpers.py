from pyspark.sql import SparkSession

def get_catalog() -> str:

    spark = SparkSession.getActiveSession()
    return spark.conf.get("frontrunner.catalog", "dev")

def landing_path(subfolder: str) -> str:
    """Builds a path into this project's landing volume for a given source subfolder."""
    return f"/Volumes/{get_catalog()}/frontrunner/landing/{subfolder}"    