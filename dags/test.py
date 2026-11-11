from airflow import DAG
from datetime import datetime
from dags.Proj_1.operators.Postgres_operator import YariloApitoPostgresOperator

DEFAULT_ARGS = {
    'owner': 'YARILO',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 12),
}
with DAG(
    dag_id="test",
    tags=['yarilo'],
    schedule='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    test = YariloApitoPostgresOperator(task_id='test',date_from = '{{ ds }}',date_to = '{{ macros.ds_add(ds, 1) }}')
