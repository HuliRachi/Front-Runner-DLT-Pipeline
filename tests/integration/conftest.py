import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient

TARGET = "uat"
 
@dataclass
class UatResourceIds:
    ingestion_pipeline_id: str
    transformation_pipeline_id: str
    orchestration_job_id: str
    warehouse_id: str
 
 
@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    """
    Generates an authorized Databricks SDK WorkspaceClient instance.
    """
    return WorkspaceClient()
 
 
@pytest.fixture(scope="session")
def uat_resource_ids(workspace_client: WorkspaceClient) -> UatResourceIds:
    """
    Looks up deployment resource IDs natively using the Databricks Python SDK,
    matching your exact server names containing dashes.
    """
    ingestion_id = None
    transformation_id = None
    orchestration_id = None
    warehouse_id = None

    try:
        # 1. Fetch Pipeline IDs dynamically using exact dash matching
        for p in workspace_client.pipelines.list_pipelines():
            name_lower = p.name.lower()
            if "ingestion-pipeline" in name_lower and "frontrunner" in name_lower:
                ingestion_id = p.pipeline_id
            elif "transformation-pipeline" in name_lower and "frontrunner" in name_lower:
                transformation_id = p.pipeline_id
                
        # 2. Fetch Orchestration Job ID using exact dash matching
        for j in workspace_client.jobs.list():
            current_name = ""
            if hasattr(j, "settings") and j.settings and hasattr(j.settings, "name"):
                current_name = j.settings.name
            elif hasattr(j, "job_name") and j.job_name:
                current_name = j.job_name
                
            name_lower = current_name.lower()
            if "orchestration-job" in name_lower and "frontrunner" in name_lower:
                orchestration_id = j.job_id
                break

        # 3. Fetch SQL Warehouse ID by name lookup
        for w in workspace_client.warehouses.list():
            if "warehouse" in w.name.lower() or "frontrunner" in w.name.lower():  
                warehouse_id = w.id
                break

    except Exception as e:
        pytest.fail(f"Failed to query workspace metadata using Databricks SDK: {str(e)}")

    # 4. Fallback validation checks
    missing_resources = []
    if not ingestion_id: missing_resources.append("frontrunner-ingestion-pipeline")
    if not transformation_id: missing_resources.append("frontrunner-transformation-pipeline")
    if not orchestration_id: missing_resources.append("frontrunner-orchestration-job")
    
    if missing_resources:
        actual_pipelines = [p.name for p in workspace_client.pipelines.list_pipelines()]
        actual_jobs = [j.job_name if hasattr(j, "job_name") else j.settings.name for j in workspace_client.jobs.list()]
        
        pytest.fail(
            f"Could not locate deployed assets containing keys: {missing_resources}.\n"
            f"Actual pipelines found in workspace: {actual_pipelines}\n"
            f"Actual jobs found in workspace: {actual_jobs}"
        )

    # Fallback to your warehouse parameter from databricks.yml if name isn't matched
    if not warehouse_id:
        warehouse_id = "7ffa80bc21a73630"

    return UatResourceIds(
        ingestion_pipeline_id=ingestion_id,
        transformation_pipeline_id=transformation_id,
        orchestration_job_id=orchestration_id,
        warehouse_id=warehouse_id,
    )
 
 
@pytest.fixture(scope="function")
def reset_uat(workspace_client: WorkspaceClient):
    """
    Cleans up landing volumes before test execution.
    """
    catalog = TARGET  
    landing_root = f"/Volumes/{catalog}/frontrunner/landing"
    subfolders = ["customers_cdc", "products", "clickstream"]
    
    for folder in subfolders:
        dir_path = f"{landing_root}/{folder}"
        try:
            for entry in workspace_client.files.list_directory_contents(dir_path):
                workspace_client.files.delete(f"{dir_path}/{entry.name}")
        except Exception:
            pass
 
    yield
