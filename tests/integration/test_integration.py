import os
import subprocess
import pytest

# --- CONFIGURABLE TARGETS: Tiny sample data profiles for fast E2E validation ---
EXPECTED_LANDING_COUNTS = {
    "customers": 2,
    "products": 3,
    "clickstream": 5,
}

def _generate_fixed_seed_batch() -> str:
    """
    Executes the data generator via a CLI subprocess, overriding target limits
    dynamically to generate small test slices rather than production scales.
    """
    result = subprocess.run(
        [
            "python3", "../../data_generator.py",
            f"--n-customers={EXPECTED_LANDING_COUNTS['customers']}",
            f"--n-products={EXPECTED_LANDING_COUNTS['products']}",
            f"--n-clickstream={EXPECTED_LANDING_COUNTS['clickstream']}",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"data_generator.py execution failed:\n{result.stderr}"
    
    # Returns the hardcoded directory name created by the generator script
    return "batch_01"


def _upload_batch_to_landing(workspace_client, batch_dir: str, catalog: str):
    """
    Scans the local generated batch directories and syncs them to the 
    Databricks Unity Catalog Landing Volume endpoint.
    """
    landing_root = f"/Volumes/{catalog}/frontrunner/landing"
    
    if not os.path.exists(batch_dir):
        raise FileNotFoundError(f"Target batch generation path not found: {batch_dir}")
        
    for folder in os.listdir(batch_dir):
        local_folder = os.path.join(batch_dir, folder)
        if not os.path.isdir(local_folder):
            continue
            
        for filename in os.listdir(local_folder):
            local_path = os.path.join(local_folder, filename)
            remote_path = f"{landing_root}/{folder}/{filename}"
            
            with open(local_path, "rb") as f:
                workspace_client.files.upload(remote_path, f, overwrite=True)


def _table_count_safe(workspace_client, catalog: str, warehouse_id: str, table: str) -> int:
    """
    Queries table volumes via the Databricks SQL Statement Execution API.
    Returns 0 safely if target quarantine tables have not been created yet by DLT.
    """
    try:
        result = workspace_client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema="frontrunner",
            statement=f"SELECT COUNT(*) FROM {table}",
            wait_timeout="30s",
        )
        if result.status.state.value == "SUCCEEDED":
            # Extract row integer value out of the nested result array payload
            return int(result.result.data_array[0][0])
        return 0
    except Exception:
        # Gracefully handles lazy-created DLT tables that are missing from disk
        return 0


def test_orchestration_job_end_to_end(
    workspace_client, uat_resource_ids, reset_uat
):
    """
    Core End-to-End Integration Test:
    Generates minimal data, uploads it to landing volumes, triggers DLT pipeline 
    updates sequentially, and validates complete data conservation across layers.
    """
    # 1. Generate minor sample data batch volumes
    batch_dir = _generate_fixed_seed_batch()
    
    # 2. Upload assets directly into UAT Landing Zone Volume
    catalog = "uat"
    _upload_batch_to_landing(workspace_client, batch_dir, catalog=catalog)
 
    # 3. Trigger Ingestion DLT Pipeline (Bronze Layer)
    workspace_client.pipelines.start_update(
        pipeline_id=uat_resource_ids.ingestion_pipeline_id, full_refresh=True
    )
    workspace_client.pipelines.wait_get_pipeline_idle(
        pipeline_id=uat_resource_ids.ingestion_pipeline_id
    )
 
    # 4. Trigger Transformation DLT Pipeline (Silver & Gold Layers)
    workspace_client.pipelines.start_update(
        pipeline_id=uat_resource_ids.transformation_pipeline_id, full_refresh=True
    )
    workspace_client.pipelines.wait_get_pipeline_idle(
        pipeline_id=uat_resource_ids.transformation_pipeline_id
    )
 
    # 5. Run the master wrapper Orchestration Job task
    run = workspace_client.jobs.run_now(
        job_id=int(uat_resource_ids.orchestration_job_id),
    ).result()
 
    assert run.state.result_state.value == "SUCCESS", (
        f"Master orchestration run failed: {run.state.state_message}"
    )
 
    # 6. --- Data Conservation Volume Assertions ---
    for source, expected_total in EXPECTED_LANDING_COUNTS.items():
        valid = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_valid"
        )
        quarantined = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined"
        )
        
        # Verify that total parsed rows equal the initial generated batch total
        assert valid + quarantined == expected_total, (
            f"Row mismatch on bronze_{source}! Got {valid + quarantined} rows, "
            f"expected {expected_total}. System dropped records during delta loads."
        )

    # 7. --- Data Quality Clean-Run Assertions ---
    for source in EXPECTED_LANDING_COUNTS.keys():
        quarantined = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined"
        )
        assert quarantined == 0, (
            f"Data quality error! Table bronze_{source}_quarantined contains "
            f"{quarantined} items. Expected a completely clean simulation run."
        )
