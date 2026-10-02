import os
import subprocess
import pytest

# --- ALIGNED TARGETS: Matches your generator's hardcoded outputs exactly ---
EXPECTED_LANDING_COUNTS = {
    "customers": 15,    # Aligned with TOTAL_CUSTOMERS = 15
    "products": 20,     # Aligned with TOTAL_PRODUCTS = 20
    "clickstream": 55,  # Aligned with TOTAL_EVENTS = 55
}

def _generate_fixed_seed_batch() -> str:
    """
    Executes the data generator via a CLI subprocess.
    """
    # Call the generator cleanly without flags to prevent script execution issues
    result = subprocess.run(
        ["python3", "../../data_generator.py"],
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
    Measures the baseline record volume before execution, uploads new data, 
    and checks that the final volume grows by precisely the new batch size.
    """
    catalog = "uat"
    landing_root = f"/Volumes/{catalog}/frontrunner/landing"
    subfolders = ["customers_cdc", "products", "clickstream"]
    
    # 1. Clean landing volumes to ensure only current batch files exist
    for folder in subfolders:
        dir_path = f"{landing_root}/{folder}"
        try:
            for entry in workspace_client.files.list_directory_contents(dir_path):
                workspace_client.files.delete(f"{dir_path}/{entry.name}")
        except Exception:
            pass

    # 2. Capture the baseline counts *before* the job executes incremental updates
    initial_counts = {}
    for source in EXPECTED_LANDING_COUNTS.keys():
        v_count = _table_count_safe(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_valid")
        q_count = _table_count_safe(workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined")
        initial_counts[source] = v_count + q_count

    # 3. Generate sample data
    batch_dir = _generate_fixed_seed_batch()
    
    # 4. Upload files to the Landing Volume
    _upload_batch_to_landing(workspace_client, batch_dir, catalog=catalog)
 
    # 5. Trigger the master Orchestration Job
    run = workspace_client.jobs.run_now(
        job_id=int(uat_resource_ids.orchestration_job_id),
    ).result()
 
    assert run.state.result_state.value == "SUCCESS", (
        f"Master orchestration run failed: {run.state.state_message}"
    )
 
    # 6. --- Production-Grade Relative Delta Volume Assertions ---
    for source, expected_new_rows in EXPECTED_LANDING_COUNTS.items():
        valid = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_valid"
        )
        quarantined = _table_count_safe(
            workspace_client, catalog, uat_resource_ids.warehouse_id, f"bronze_{source}_quarantined"
        )
        
        final_total = valid + quarantined
        net_new_processed = final_total - initial_counts[source]
        
        # Verify that exactly the new rows were appended cleanly
        assert net_new_processed == expected_new_rows, (
            f"Row mismatch on bronze_{source}! Expected precisely {expected_new_rows} "
            f"new records to process, but the table grew by {net_new_processed} rows."
        )
