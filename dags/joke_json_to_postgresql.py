from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta
import pendulum
import requests
import logging
import pandas as pd

default_args = {
    "owner": "kirill_fedorov",
    "retries": 0,
    "execution_timeout": timedelta(seconds=300),
}


@dag(
    'joke_dag',
    description="Парсинг шутки https://official-joke-api.appspot.com/jokes/random",
    default_args=default_args,
    #schedule="0 * * * *",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2023, 10, 25, tz="UTC"),
)
def joke_dag():
    """
    Парсинг шутки из json
    по адресу https://official-joke-api.appspot.com/jokes/random
    в таблицу airflow_studies.jokes postgresql.
    Реализована обработка конфликта по полю joke_id:
    конфликтная запись обновляется.
    """

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    @task
    def parse_api() -> pd.DataFrame:
        try:
            response = requests.get('https://official-joke-api.appspot.com/jokes/random', timeout=10)
            response.raise_for_status()

            if "setup" not in response.text:
                raise ValueError(f"Неожиданный ответ от api: {response.text}")
            
            df = pd.DataFrame([response.json()])
        except Exception as e:
            logging.error(e)
            raise
        else:
            return df

    @task
    def upload_to_stage(df: pd.DataFrame) -> None:
        hook = PostgresHook(postgres_conn_id='postgres')

        df.to_sql(
            con=hook.get_uri(),
            schema='airflow_studies',
            name='stage_jokes',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000,
        )

    stage_to_main = SQLExecuteQueryOperator(
        task_id='stage_to_main',
        sql='''
            INSERT INTO airflow_studies.jokes (joke_id, joke_type, setup, punchline)
            SELECT id, type, setup, punchline FROM airflow_studies.stage_jokes
            ON CONFLICT (joke_id) DO UPDATE SET
                joke_type = EXCLUDED.joke_type,
                setup = EXCLUDED.setup,
                punchline = EXCLUDED.punchline;
            ''',
        conn_id='postgres',
    )

    parsed_data = parse_api()

    parsed_to_stage = upload_to_stage(parsed_data)

    _ = start >> parsed_to_stage >> stage_to_main >> end

joke_dag()