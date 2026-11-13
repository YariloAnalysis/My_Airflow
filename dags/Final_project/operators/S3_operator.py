
from typing import Any
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
class S3_operator(BaseOperator):
    def __init__(self,table_name,**kwargs):
        self.table_name = table_name
        super().__init__(**kwargs)
    def execute(self, context: Any):
        import codecs
        import csv
        from io import BytesIO
        import boto3 as s3
        from botocore.client import Config
# Подключаюсь к БД через Хук и формирую csv файл
        hook = PostgresHook(postgres_conn_id='to_pg')
        result = hook.get_records(
        sql = f'select * from {self.table_name}',
        )
        file = BytesIO()

        writer_wrapper = codecs.getwriter('utf-8')

        writer = csv.writer(
            writer_wrapper(file),
        delimiter='\t',
        lineterminator='\n',
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL
    )

        writer.writerows(result)
        file.seek(0)

        connection = BaseHook.get_connection('conn_s3')

        s3_client = s3.client(
            's3',
            endpoint_url=connection.host,
            aws_access_key_id=connection.login,
            aws_secret_access_key=connection.password,
            config=Config(signature_version="s3v4"),
    )

        s3_client.put_object(
            Body=file,
            Bucket='default-storage',
            Key=f"yarilo_final_{self.table_name}_{context['ds']}.csv"
    )

