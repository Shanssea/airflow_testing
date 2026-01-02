from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

class PostgresToCSVHook(PostgresHook):
    def __init__(
            self,
            output_path,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.output_path = output_path

    def _process_output(self, results):
        """Overrides the internal method to save query results to CSV.
        This method is recommended by SQLExecuteQueryOperator documentation to process the output.
        """
        df = pd.DataFrame(results)
        df.to_csv(self.output_path, index=False)
        return results