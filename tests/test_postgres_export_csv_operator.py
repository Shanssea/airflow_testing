import pytest
from unittest.mock import patch
from operators.postgres_export_csv_operator import PostgresExportCSVOperator
    
@pytest.fixture
def default_args():
    return {
        "task_id": "test_export",
        "output_path": "/tmp/test_output.csv",
        "conn_id": "postgres_default",
        "sql": "testing.sql"
    }
@patch("operators.postgres_export_csv_operator.PostgresToCSVHook")
def test_get_db_hook(mock_hook, default_args):
    """Test if get_db_hook returns the custom PostgresToCSVHook with correct params."""
    op = PostgresExportCSVOperator(**default_args)

    hook = op.get_db_hook()
    
    mock_hook.assert_called_once_with(
        postgres_conn_id=default_args["conn_id"],
        output_path=default_args["output_path"]
    )

    assert hook == mock_hook.return_value