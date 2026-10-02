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
            return int(result.result.data_array[0][0])
        return 0
    except Exception:
        return 0


def test_orchestration_job_end_to_end(
    workspace_client, uat_resource_ids
):
    """
    Core End-to-End Integration Test:
    Cleans landing volumes, uploads fresh test data, and triggers ONLY the
    master orchestration job to run the complete end-to-end data pipeline.
    """
    catalog = "uat"
    landing_root = f"/Volumes/{catalog}/frontrunner/landing"
    subfolders = ["customers_cdc", "products", "clickstream"]
    
    # 1. Clean the Volume folders to remove old test runs
    for folder in subfolders:
        dir_path = f"{landing_root}/{folder}"
        try:
            for entry in workspace_client.files.list_directory_contents(dir_path):
                workspace_client.files.delete(f"{dir_path}/{entry.name}")
        except Exception:
            pass

    # 2. Generate small sample test data
    batch_dir = _generate_fixed_seed_batch()
    
    # 3. Upload files to the Landing Volume
    _upload_batch_to_landing(workspace_client, batch_dir, catalog=catalog)
 
    # 4. Trigger ONLY the master Orchestration Job
    # This automatically invokes the underlying pipelines in the correct order
    run = workspace_client.jobs.run_now(
        job_id=int(uat_resource_ids.orchestration_job_id),
    ).result()
 
    assert run.state.result_state.value == "SUCCESS", (
        f"Master orchestration run failed: {run.state.state_message}"
    )
 
    # 5. --- Data Conservation Volume Assertions ---
    for source, expected_total in EXPECTED_LANDING_COUNTS.items():
        valid = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_valid"
        )
        quarantined = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined"
        )
        
        # Verify that total parsed rows equal the initial generated batch total exactly
        assert valid + quarantined == expected_total, (
            f"Row mismatch on bronze_{source}! Got {valid + quarantined} rows, "
            f"expected {expected_total}. System dropped records during delta loads."
        )

    # 6. --- Data Quality Clean-Run Assertions ---
    for source in EXPECTED_LANDING_COUNTS.keys():
        quarantined = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined"
        )
        assert quarantined == 0, (
            f"Data quality error! Table bronze_{source}_quarantined contains "
            f"{quarantined} items. Expected a completely clean simulation run."
        )
