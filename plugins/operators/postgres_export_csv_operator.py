from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd
from pathlib import Path

class PostgresExportCSVOperator(SQLExecuteQueryOperator):
    def __init__(
            self,
            output_path,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.output_path = output_path
        
    def _process_output(self, results, context):
        """Overrides the internal method to save query results to CSV.
        This method is recommended by SQLExecuteQueryOperator documentation to process the output.
        """
        output_path = Path(self.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame(results)
        df.to_csv(self.output_path, index=False)
        
        return results