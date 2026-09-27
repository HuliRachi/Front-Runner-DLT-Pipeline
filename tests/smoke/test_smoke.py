 
GOLD_TABLES = [
    "gold_product_performance",
    "gold_customer_360",
    
]
 
 
def test_pipelines_exist_and_are_reachable(workspace_client, prod_resource_ids):
    for pipeline_id in (
        prod_resource_ids.ingestion_pipeline_id,
        prod_resource_ids.transformation_pipeline_id,
    ):
        pipeline = workspace_client.pipelines.get(pipeline_id=pipeline_id)
        assert pipeline is not None, f"Pipeline {pipeline_id} not reachable"
 
 
def test_pipelines_latest_update_did_not_fail(workspace_client, prod_resource_ids):
   
    for pipeline_id in (
        prod_resource_ids.ingestion_pipeline_id,
        prod_resource_ids.transformation_pipeline_id,
    ):
        response = workspace_client.pipelines.list_updates(
            pipeline_id=pipeline_id, max_results=1
        )
        updates = response.updates or []
        if not updates:
            print(f"Pipeline {pipeline_id}: no update history yet — skipping, not a failure")
            continue
 
        latest = updates[0]
        assert latest.state.value not in ("FAILED", "CANCELED"), (
            f"Pipeline {pipeline_id}'s most recent update is in state "
            f"{latest.state.value}, not healthy"
        )
 
 
def test_orchestration_job_exists_and_is_reachable(workspace_client, prod_resource_ids):
    job = workspace_client.jobs.get(job_id=int(prod_resource_ids.orchestration_job_id))
    assert job is not None, "Orchestration job not reachable"
 
 
def test_dashboard_exists_and_is_reachable(workspace_client, prod_resource_ids):
    
    dashboard = workspace_client.lakeview.get(dashboard_id=prod_resource_ids.dashboard_id)
    assert dashboard is not None, "Dashboard not reachable"
 
 
def test_gold_tables_are_queryable(workspace_client, prod_resource_ids):
    
    for table in GOLD_TABLES:
        result = workspace_client.statement_execution.execute_statement(
            warehouse_id=prod_resource_ids.warehouse_id,
            catalog="prod",
            schema="frontrunner",
            statement=f"SELECT COUNT(*) FROM {table}",
            wait_timeout="30s",
        )
        assert result.status.state.value == "SUCCEEDED", (
            f"{table} is not queryable: {result.status.error}"
        )
        row_count = int(result.result.data_array[0][0])
        print(f"{table}: {row_count} rows (informational only, not asserted)")
 