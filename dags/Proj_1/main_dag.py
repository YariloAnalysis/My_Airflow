
from datetime import datetime,timedelta
from botocore.client import Config
import boto3
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.empty import EmptyOperator
from Proj_1.operators import YariloApitoPostgresOperator, S3_operator, CustomBranchOperator

DEFAULT_ARGS = {
    'owner': 'YARILO',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}
class WeekTemplates:
    @staticmethod
    def current_week_start(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        current_week_start = logical_dt - timedelta(days=logical_dt.weekday())

        return current_week_start.strftime("%Y-%m-%d")

    @staticmethod
    def current_week_end(date) -> str:
        logical_dt = datetime.strptime(date, "%Y-%m-%d")

        current_week_end = logical_dt + timedelta(days=6 - logical_dt.weekday())

        return current_week_end.strftime("%Y-%m-%d")
with DAG(
    dag_id="combine_api_data",
    tags=['yarilo'],
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1,
    user_defined_macros={
        "current_week_start": WeekTemplates.current_week_start,
        "current_week_end": WeekTemplates.current_week_end,
        }
) as dag:
    start_dag = EmptyOperator(task_id='start_dag')
    end_dag = EmptyOperator(task_id='end_dag')
    api_to_pg_operator = YariloApitoPostgresOperator(task_id='load_from_api',date_from = '{{ ds }}',date_to = '{{ macros.ds_add(ds, 1) }}')
    sql_sensor = SqlSensor(task_id='sql_sensor',conn_id = 'to_pg',sql = 'select exists ( select 1 from yarilo_table)',mode='reschedule')
    branch_operator = CustomBranchOperator(task_id = 'branch',target_task = 'agregate_to_s3',work_day = (1,2,5))
    agregate_to_s3 =S3_operator(task_id = 'agregate_to_s3',date_from = '{{current_week_start(ds)}}',date_to = '{{current_week_end(ds)}}')
    start_dag >> api_to_pg_operator >> sql_sensor >> branch_operator >> agregate_to_s3 >> end_dag
