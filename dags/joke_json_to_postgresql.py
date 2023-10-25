from datetime import timedelta
from airflow.decorators import dag, task
import pendulum
from airflow.operators.empty import EmptyOperator
import requests
import json
import logging
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

JOKE_API_URL = "https://official-joke-api.appspot.com/jokes/random"
DB_TABLE = "airflow_studies.jokes"

default_args = {
    "owner": "kirill_fedorov",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "timeout": 300,
}


@dag(
    default_args=default_args,
    description=f"Парсинг шутки {JOKE_API_URL}",
    schedule="@hourly",
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
)
def joke_dag():
    f"""
    Парсинг шутки из json
    по адресу {JOKE_API_URL}
    в таблицу {DB_TABLE} postgresql.
    Реализована обработка конфликта по полю joke_id:
    конфликтная запись обновляется.
    """

    start = EmptyOperator(task_id="start")

    @task
    def parse_api() -> dict:
        try:
            response = requests.get(JOKE_API_URL, timeout=10)
            response.raise_for_status()
            if "setup" not in response.text:
                raise ValueError(f"Неожиданный ответ от api: {response.text}")
            joke_data = json.loads(response.text)
        except (requests.RequestException, ValueError) as e:
            logging.error(e)
            raise
        else:
            return joke_data

    parse_api_py = parse_api()

    upload_to_db = SQLExecuteQueryOperator(
        task_id="upload_to_db",
        conn_id="postgres",
        sql=f"""
            INSERT INTO {DB_TABLE} (joke_id, joke_type, setup, punchline)
            VALUES (%(joke_id)s, %(joke_type)s, %(setup)s, %(punchline)s)
            ON CONFLICT (joke_id) DO UPDATE SET
                joke_type = %(joke_type)s,
                setup = %(setup)s,
                punchline = %(punchline)s;
        """,
        parameters={
            "joke_id": "{{ ti.xcom_pull(task_ids='parse_api')['id'] }}",
            "joke_type": "{{ ti.xcom_pull(task_ids='parse_api')['type'] }}",
            "setup": "{{ ti.xcom_pull(task_ids='parse_api')['setup'] }}",
            "punchline": "{{ ti.xcom_pull(task_ids='parse_api')['punchline'] }}",
        },
    )

    end = EmptyOperator(task_id="end")

    _ = start >> parse_api_py >> upload_to_db >> end


dag = joke_dag()
