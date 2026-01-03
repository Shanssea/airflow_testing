from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pathlib import Path

class PostgresExportCSVOperator(SQLExecuteQueryOperator):
    template_fields = ('output_path', *SQLExecuteQueryOperator.template_fields) # Make sure jinjja can be rendered in output_path
    def __init__(
            self,
            output_path,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.output_path = output_path
        
    def _process_output(self, results, description):
        """Overrides the internal method to save query results to CSV.
        This method is recommended by SQLExecuteQueryOperator documentation to process the output.
        """
    
        header = [col.name for col in description[0]]
        df = pd.DataFrame(results[0], columns=header)
        df.to_csv(self.output_path, header=header, index=False)

        return results