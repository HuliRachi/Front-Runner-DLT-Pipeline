from pyspark import pipelines as dp

from utilities.dq_alert_logic import check_expectations_and_alert


@dp.on_event_hook(max_allowable_consecutive_failures=3)
def dq_expectation_alert_hook(event: dict) -> None:
    check_expectations_and_alert(event, pipeline_label="ingestion")
