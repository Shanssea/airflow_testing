from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from hooks.postgres_to_csv_hook import PostgresToCSVHook

class PostgresExportCSVOperator(SQLExecuteQueryOperator):
    def __init__(
            self,
            output_path,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.output_path = output_path

    def get_db_hook(self):
        """Overrides the internal method to use PostgresToCSVHook."""
        return PostgresToCSVHook(
            output_path=self.output_path,
            postgres_conn_id=self.conn_id
        )