import yaml
from pathlib import Path
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from Final_project.operators.S3_operator import S3_operator
# добавил в конфиг WHERE created_at = {{ ds }} для фильтрации Jinja а также изменил название стобцов чтобы запросы к БД прошли корректно-использовал данные с прошлых практик по Airflow
config_path = Path(__file__).parent/'config.yaml'
DEFAULT_ARGS = {
    'owner': 'YARILO',
    'retries': 2,
    'retry_delay': 600,
    'start_date': datetime(2024, 11, 1), #В БД лежат данные с 1 по 4 число
}

with open(config_path, 'r') as ymlfile:
    cfg = yaml.safe_load(ymlfile)

with DAG(
    dag_id="FINAL",
    tags=['yarilo'],
    schedule='@once',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=1
) as dag:
    start_dag = EmptyOperator(task_id='start')
    end_dag = EmptyOperator(task_id='end')
    for task in cfg:
        ddl_task = SQLExecuteQueryOperator(
            task_id = f'create_table_{task.get("table_name")}',
            conn_id = 'to_pg',
            sql = task.get('table_ddl'),
        )
        dml_task = SQLExecuteQueryOperator(
            task_id = f'insert_to_{task.get("table_name")}',
            conn_id = 'to_pg',
            sql = f"""
            TRUNCATE TABLE {task.get("table_name")};
            INSERT INTO {task.get("table_name")} {task.get('table_dml')};
            """
        )
        start_dag>>ddl_task >> dml_task
        if task.get('need_to_export'):
            upload_task = S3_operator(task_id=f'upload_{task.get("table_name")}',table_name = f'{task.get("table_name")}')
            dml_task >> upload_task >> end_dag
        else:
            dml_task >> end_dag

