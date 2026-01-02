"""
### DAG Documentation
This DAG will fetch data from external PostgreSQL database (locally set up), 
run query, and generate csv file with that data.
The csv file then wil be sent to designated email.
"""

from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator

from operators.postgres_export_csv_operator import PostgresExportCSVOperator
from utils.config_json_loader import ConfigJSONLoader

from pathlib import Path

import os
import json
import pendulum

# Load default global config
ENV = Variable.get("ENVIRONMENT")

# Load environment config
script_dir = Path(__file__).resolve()
config = ConfigJSONLoader(script_dir)

# DAG initiation
@dag(
    dag_id=config.dag.get("dag_id"),
    schedule=config.dag.get("schedule"),
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    template_searchpath=[os.path.join(script_dir.parent, "sql")],
    tags=config.dag.get("tags")
)
def generate_report():
    dag.doc_md = __doc__

    start = EmptyOperator(task_id='start')
    skip = EmptyOperator(task_id='skip')
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    # We will check for data availability first 
    @task.branch(task_id="check_data_availability")
    def check_data_availability():
        """
        Check if rental data exists for the current month.
        """
        hook = PostgresHook(postgres_conn_id=config.postgres.get("conn_id"))
        sql = f"""
            SELECT COUNT(*)
            FROM rental
            WHERE rental_date >= DATE_TRUNC('month', '{config.sql.get("time_machine_today")}'::DATE)
              AND rental_date < DATE_TRUNC('month', '{config.sql.get("time_machine_today")}'::DATE) + INTERVAL '1 month';
        """
        records = hook.get_first(sql)
        count = records[0] if records else 0
        if count > 0:
            return "fetch_and_export"
        return "skip"

    # Fetch data from PostgreSQL
    fetch_and_export = PostgresExportCSVOperator(
        task_id="fetch_and_export",
        output_path=os.path.join(config.postgres.get("report_output_path"), 'report_{{ ds }}.csv'),
        conn_id=config.postgres.get("conn_id"),
        sql="return_status.sql"
    )

    # Send email with the report attached
    send_email = EmailOperator(
        task_id="send_email",
        to=config.email.get("recipient_email"),
        subject="MTD Rental Report",
        html_content="Please find the attached rental report.",
        files=[os.path.join(config.postgres.get("report_output_path"), 'report_{{ ds }}.csv')]
    )

    # Task dependencies
    start >> check_data_availability() >> [fetch_and_export, skip]
    fetch_and_export >> send_email >> end
    skip >> end

generate_report()