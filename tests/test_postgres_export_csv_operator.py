import pytest
import pandas as pd
from unittest.mock import patch
from operators.postgres_export_csv_operator import PostgresExportCSVOperator

@pytest.fixture
def mock_operator_instance():
    """Fixture to initialize the hook with a dummy path."""
    return PostgresExportCSVOperator(
        output_path="/tmp/mock_output.csv", 
        conn_id="postgres_test",
        sql="testing.sql",
        task_id="test_task"
    )

@pytest.mark.parametrize("query_results, expected_df_length", [
    ([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], 2),
    ([{"id": 100, "status": "active"}], 1),
    ([], 0)
])
@patch("pandas.DataFrame.to_csv")
def test_process_output(mock_to_csv, mock_operator_instance, query_results, expected_df_length):
    """Verify that results are converted to DataFrame and saved to CSV."""
    result = mock_operator_instance._process_output(query_results, context={})
    
    assert result == query_results # Ensure original results are returned
    args, kwargs = mock_to_csv.call_args # Capture args and kwargs used in to_csv
    assert args[0] == mock_operator_instance.output_path 
    assert kwargs["index"] is False
    assert len(pd.DataFrame(result)) == expected_df_length