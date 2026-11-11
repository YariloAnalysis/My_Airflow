import ast
import psycopg2 as pg
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator

class YariloApitoPostgresOperator(BaseOperator):
    API = "https://api.itresume.ru/statistics/"

    template_fields = ('date_from', 'date_to')
    def __init__(self,date_from,date_to,**kwargs):
        self.date_from = date_from
        self.date_to = date_to
        super().__init__(**kwargs)
    def execute(self, context):
        payload = {
            'client': 'Skillfactory',
            'client_key': 'M2MGWS',
            'start': self.date_from,
            'end': self.date_to,
        }
        response = requests.get(self.API,params=payload)
        data = response.json()
        rows = []

        for el in data:
            raw_params = el.get("passback_params")


            passback_params = (
                ast.literal_eval(raw_params)
                if isinstance(raw_params, str) and raw_params
                else raw_params or {}
            )

            rows.append((
                el.get("lti_user_id"),
                el.get("is_correct") == 1,
                el.get("attempt_type"),
                el.get("created_at"),
                passback_params.get("oauth_consumer_key"),
                passback_params.get("lis_result_sourcedid"),
                passback_params.get("lis_outcome_service_url"),
            ))

        if rows:
            hook = PostgresHook(postgres_conn_id="to_pg")

            hook.insert_rows(
                table="yarilo_table",
                rows=rows,
                executemany=True,
                commit_every=1000,
            )

            self.log.info("Вставлено записей: %s", len(rows))
        else:
            self.log.info("API вернул пустой список — вставлять нечего")