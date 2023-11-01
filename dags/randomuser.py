from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta
import logging
import pendulum
import requests
import pandas as pd

default_args = {
    "owner": "kirill_fedorov",
    "retries": 0,
    "execution_timeout": timedelta(seconds=300),
}


@dag(
    'randomuser_parse',
    default_args=default_args,
    description="Генерация случайной личности из api https://randomuser.me/api/",
    start_date=pendulum.datetime(2023, 10, 25, tz="UTC"),
    #schedule="*/5 * * * *",
    schedule_interval=None,
)
def randomuser_parse():
    """
    Генерация случайной личности из api https://randomuser.me/api/
    в таблицы randomuser.user, randomuser.location, randomuser.login.
    Реализована обработка конфликта по полю uuid:
    случайная личность генерируется заново.
    """
    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    @task
    def parse_api() -> pd.DataFrame:
        try:
            response = requests.get('https://randomuser.me/api/', timeout=10)
            response.raise_for_status()

            if "results" not in response.text:
                raise ValueError(f"Неожиданный ответ от api: {response.text}")
            
            df = pd.json_normalize(response.json()['results'])

            # замена спецсимволов в названиях полей таблицы
            df.columns = df.columns.str.replace(r'[\/\(\),\'"!@#$%^&*+=?<>|:;\[\]{}]', '', regex=True)
            df.columns = df.columns.str.replace(r'[ .-]', '_', regex=True)
            
            # Добавление user_id в sa - это будет копия поля id serial pkey из ha
            df['user_id'] = None
            df['user_id'] = df['user_id'].astype('Int64')
        except (requests.RequestException, ValueError) as e:
            logging.error(e)
            raise
        else:
            return df
        
    @task
    def upload_to_sa(df: pd.DataFrame):
        hook = PostgresHook(postgres_conn_id="postgres")

        df.to_sql(
            con=hook.get_uri(),
            schema="sa",
            name="randomuser",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

    sa_to_ha = SQLExecuteQueryOperator(
        task_id="stage_to_history",
        conn_id="postgres",
        sql='/randomuser/sa_to_ha.sql'
    )

    parsed_data = parse_api()

    parsed_to_sa = upload_to_sa(parsed_data)

    _ = start >> parsed_to_sa >> sa_to_ha >> end


randomuser_parse()