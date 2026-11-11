
from typing import Any
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
class S3_operator(BaseOperator):
    template_fields = ('date_from', 'date_to')
    def __init__(self,date_from,date_to,**kwargs):
        self.date_from = date_from
        self.date_to = date_to
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
        sql = """SELECT lti_user_id,
               attempt_type,
               COUNT(1),
               COUNT(CASE WHEN is_correct THEN NULL ELSE 1 END) AS attempt_failed_count
          FROM yarilo_table
         WHERE created_at >= %s::timestamp 
               AND created_at < %s::timestamp + INTERVAL '1 days'
          GROUP BY lti_user_id, attempt_type;""",
        parameters = (self.date_from, self.date_to))
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
            Key=f"YARILO_{context['ds']}.csv"
    )

