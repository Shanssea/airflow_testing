import pytest
import json
import os
from unittest.mock import patch, mock_open
from airflow.models import DagBag, Connection

# Mock config data
MOCK_CONFIG = {
    "dag": {
        "dag_id": "generate_report",
        "schedule": "@weekly",
        "tags": ["test", "test_tags"]
    },
    "postgres": {
        "report_output_path": "/opt/airflow/outputs/",
        "conn_id": "dvdrentals"
    },
    "email": {
        "recipient_email": "test@example.com"
    },
    "sql": {
        "time_machine_today": "2007-07-07"
    }
}

@pytest.fixture(autouse=True)
@patch("utils.config_json_loader.open", new_callable=mock_open, read_data=json.dumps(MOCK_CONFIG))
@patch("airflow.models.Variable.get")
def mock_airflow_variables(mock_var_get, mock_file_open):
    """
    Fixture to mock Airflow Variables and Config file loading.
    Decorators handle the setup and teardown automatically.
    """
    # Mock the Variable.get calls
    mock_vars = {"ENVIRONMENT": "dev"}
    mock_var_get.side_effect = mock_vars.get

    # Mock the config file loading
    mock_file_open.return_value.read.return_value = json.dumps(MOCK_CONFIG)

    # Yield control to the tests
    yield

@pytest.fixture
def dagbag():
    """Fixture to load the DagBag."""
    return DagBag(dag_folder=".", include_examples=False)

### DAG loading test
def test_dag_loading(dagbag):
    """Check if the DAG is loaded without syntax errors."""
    dag = dagbag.get_dag(dag_id=MOCK_CONFIG["dag"]["dag_id"])
    assert dagbag.import_errors == {} # Make sure no import errors
    assert dag is not None # Make sure DAG is loaded

### Unittest branch logic with different inputs
# Parameterized is used to test multiple inputs with each has expected outputs
@pytest.mark.parametrize("record_count, expected_branch", [
    ([5], "fetch_and_export"),
    ([0], "skip"),
    (None, "skip")
])
@patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_first")
def test_branch_logic(mock_get_first, dagbag, record_count, expected_branch):
    """Test the Python logic inside the branch task."""
    dag = dagbag.get_dag(dag_id=MOCK_CONFIG["dag"]["dag_id"])
    branch_task = dag.get_task("check_data_availability")
    
    # Mock the PostgresHook
    mock_get_first.return_value = record_count
    result = branch_task.python_callable()
    assert result == expected_branch

### Unittest custom operator
# This test only check param initiation for custom operator
# Testing internal logic of custom operator is in test_postgres_export_csv_operator.py
def test_custom_operator(dagbag):
    """Verify custom PostgresExportCSVOperator is correctly used."""
    from operators.postgres_export_csv_operator import PostgresExportCSVOperator
    dag = dagbag.get_dag(dag_id=MOCK_CONFIG["dag"]["dag_id"])
    task = dag.get_task("fetch_and_export")
    
    assert isinstance(task, PostgresExportCSVOperator)
    assert os.path.join(MOCK_CONFIG["postgres"]["report_output_path"], 'report_{{ ds }}.csv') in task.output_path

### Unittest task dependency
def test_task_dependencies(dagbag):
    """Verify the flow of the DAG."""
    dag = dagbag.get_dag(dag_id=MOCK_CONFIG["dag"]["dag_id"])

    # Desired task dependencies
    # start 
    # -> check_data_availability 
    # -> (
    #   fetch_and_export -> send_email -> end
    #   OR
    #   skip -> end
    # )

    assert "check_data_availability" in dag.get_task("start").downstream_task_ids
    assert "fetch_and_export" in dag.get_task("check_data_availability").downstream_task_ids
    assert "skip" in dag.get_task("check_data_availability").downstream_task_ids
    assert "send_email" in dag.get_task("fetch_and_export").downstream_task_ids
    assert "end" in dag.get_task("send_email").downstream_task_ids
    assert "end" in dag.get_task("skip").downstream_task_ids