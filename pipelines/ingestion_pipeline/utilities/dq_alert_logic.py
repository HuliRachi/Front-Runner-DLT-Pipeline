FAILURE_RATE_ALERT_THRESHOLD = 25.0 


def send_alert(message: str) -> None:
    print(f"[DQ ALERT] {message}")


def check_expectations_and_alert(event: dict, pipeline_label: str) -> None:

    if event.get("event_type") != "flow_progress":
        return

    details = event.get("details", {})
    flow_progress = details.get("flow_progress", {})
    data_quality = flow_progress.get("data_quality", {})
    expectations = data_quality.get("expectations", [])

    for exp in expectations:
        passed = exp.get("passed_records", 0)
        failed = exp.get("failed_records", 0)
        total = passed + failed
        if total == 0:
            continue

        failure_rate = (failed / total) * 100
        if failure_rate > FAILURE_RATE_ALERT_THRESHOLD:
            send_alert(
                f"[{pipeline_label}] Expectation '{exp.get('name')}' on '{exp.get('dataset')}' is "
                f"failing {failure_rate:.1f}% of rows ({failed} of {total}) — "
                f"above the {FAILURE_RATE_ALERT_THRESHOLD}% threshold."
            )
