import json
import subprocess
from dataclasses import dataclass
 
import pytest
from databricks.sdk import WorkspaceClient
 
BUNDLE_DIR = "../../bundle"import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest
from databricks.sdk import WorkspaceClient


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BUNDLE_DIR = os.path.join(PROJECT_ROOT, "bundle")

TARGET = "prod"
 
 
@dataclass
class ProdResourceIds:
    ingestion_pipeline_id: str
    transformation_pipeline_id: str
    orchestration_job_id: str
    dashboard_id: str
    warehouse_id: str
 
 
@pytest.fixture(scope="session")
def prod_resource_ids() -> ProdResourceIds:
    """
    Natively parses the production Databricks Asset Bundle deployment configurations
    to align smoke tests with active cloud infrastructure.
    """
    result = subprocess.run(
        ["databricks", "bundle", "summary", "--target", TARGET, "--output", "json"],
        cwd=BUNDLE_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"`databricks bundle summary --target {TARGET}` failed — is the "
            f"bundle actually deployed to {TARGET}?\n{result.stderr}"
        )
 
    brace_index = result.stdout.find("{")
    if brace_index == -1:
        pytest.fail(
            f"`databricks bundle summary` produced no JSON at all — full stdout:\n{result.stdout}"
        )
    summary = json.loads(result.stdout[brace_index:])
 
    try:
        resources = summary["resources"]
        # ALIGNED KEYS: Matches the exact keys deployed by your frontrunner-lakehouse configurations
        return ProdResourceIds(
            ingestion_pipeline_id=resources["pipelines"]["frontrunner_ingestion_pipeline"]["id"],
            transformation_pipeline_id=resources["pipelines"]["frontrunner_transformation_pipeline"]["id"],
            orchestration_job_id=resources["jobs"]["frontrunner_orchestration_job"]["id"],
            dashboard_id=resources["dashboards"]["frontrunner_dq_dashboard"]["id"],
            warehouse_id=summary["variables"]["frontrunner_warehouse_id"]["value"],
        )
    except KeyError as e:
        pytest.fail(
            f"Unexpected `bundle summary` JSON shape — missing key {e}. "
            f"Full output:\n{json.dumps(summary, indent=2)}"
        )
 
 
@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
    """
    Generates an authorized Databricks SDK WorkspaceClient instance.
    """
    return WorkspaceClient()

TARGET = "prod"
 
 
@dataclass
class ProdResourceIds:
    ingestion_pipeline_id: str
    transformation_pipeline_id: str
    orchestration_job_id: str
    dashboard_id: str
    warehouse_id: str
 
 
@pytest.fixture(scope="session")
def prod_resource_ids() -> ProdResourceIds:
   
    result = subprocess.run(
        ["databricks", "bundle", "summary", "--target", TARGET, "--output", "json"],
        cwd=BUNDLE_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(
            f"`databricks bundle summary --target {TARGET}` failed — is the "
            f"bundle actually deployed to {TARGET}?\n{result.stderr}"
        )
 
    brace_index = result.stdout.find("{")
    if brace_index == -1:
        pytest.fail(
            f"`databricks bundle summary` produced no JSON at all — full stdout:\n{result.stdout}"
        )
    summary = json.loads(result.stdout[brace_index:])
 
    try:
        resources = summary["resources"]
        return ProdResourceIds(
            ingestion_pipeline_id=resources["pipelines"]["frontrunner_ingestion_pipeline"]["id"],
            transformation_pipeline_id=resources["pipelines"]["frontrunner_transformation_pipeline"]["id"],
            orchestration_job_id=resources["jobs"]["frontrunner_orchestration_job"]["id"],
            dashboard_id=resources["dashboards"]["frontrunner_dq_dashboard"]["id"],
            warehouse_id=summary["variables"]["frontrunner_warehouse_id"]["value"],
        )
    except KeyError as e:
        pytest.fail(
            f"Unexpected `bundle summary` JSON shape — missing key {e}. "
            f"Full output:\n{json.dumps(summary, indent=2)}"
        )
 
 
@pytest.fixture(scope="session")
def workspace_client() -> WorkspaceClient:
   
    return WorkspaceClient()

 