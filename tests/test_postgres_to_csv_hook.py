import pytest
import pandas as pd
from unittest.mock import patch
from hooks.postgres_to_csv_hook import PostgresToCSVHook

@pytest.fixture
def mock_hook_instance():
    """Fixture to initialize the hook with a dummy path."""
    return PostgresToCSVHook(
        output_path="/tmp/mock_output.csv", 
        postgres_conn_id="postgres_test"
    )

@pytest.mark.parametrize("query_results, expected_df_length", [
    ([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], 2),
    ([{"id": 100, "status": "active"}], 1),
    ([], 0)
])
@patch("pandas.DataFrame.to_csv")
def test_process_output(mock_to_csv, mock_hook_instance, query_results, expected_df_length):
    """Verify that results are converted to DataFrame and saved to CSV."""
    
    # Mock pandas DataFrame.to_csv to avoid actual file writing
    result = mock_hook_instance._process_output(query_results)
    
    assert result == query_results # Ensure original results are returned
    args, kwargs = mock_to_csv.call_args # Capture args and kwargs used in to_csv
    assert args[0] == mock_hook_instance.output_path 
    assert kwargs["index"] is False
    assert len(pd.DataFrame(result)) == expected_df_length