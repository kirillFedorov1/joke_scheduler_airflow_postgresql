from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pendulum
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests
import pandas as pd
from io import StringIO
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    "owner": "kirill_fedorov",
    "retries": 0,
    "execution_timeout": timedelta(minutes=5),
}

# Начиная с какой даты данные загружаются в dwh (а не только в историческую таблицу)
dwh_start_date = pendulum.datetime(2000, 1, 1, tz="UTC")

yesterday_ds = "{{ macros.ds_add(ds, -1) }}"
two_days_before_ds = "{{ macros.ds_add(ds, -2)}}"

@dag(
    "earthquakes",
    default_args=default_args,
    description="""Ежедневный парсинг инфы о землятрясениях с
        https://earthquake.usgs.gov/fdsnws/event/1/.
        Нагнаны данные с минимального года, который есть в API (1661)
        """,
    start_date=pendulum.datetime(1661, 2, 11, tz="UTC"),
    schedule="0 0 * * *",
    # schedule=None,
    catchup=False,
    template_searchpath=["./earthquakes"],
)
def earthquakes():
    """
    Парсинг инфы о землятрясениях.
    Данные нагоняются с минимальной даты, которая есть в API (1661.02.11).
    Поступают в контейнер data_postgres.
    Реализованы слои:
    временная таблица,
    историческая таблица (ненормализована, партиции по годам с 2000 года),
    dwh слой (нормализация, ключи, индексы).
    """

    @task
    def parse_api(yesterday, two_days_before) -> pd.DataFrame:
        base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
        params = {
            "format": "csv",
            "starttime": two_days_before,
            "endtime": yesterday,
        }

        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))
        print(f"{base_url}?format=csv&starttime={two_days_before}&endtime={yesterday}")
        return df
    
    @task(trigger_rule="none_failed")
    def upload_to_sa(df: pd.DataFrame) -> None:
        hook = PostgresHook(postgres_conn_id="postgres")

        df.to_sql(
            con=hook.get_uri(),
            schema="sa",
            name="earthquakes",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    parsed_data = parse_api(yesterday_ds, two_days_before_ds)

    # В некоторые дни не было данных
    check_no_data = ShortCircuitOperator(
        task_id="check_no_data",
        python_callable=lambda df: not df.empty,
        op_args=[parsed_data],
    )

    parsed_to_sa = upload_to_sa(parsed_data)

    # В ha одна историческая партиция 1661-1999, далее создается по одной партиции в год
    branch_partition_ha = BranchPythonOperator(
        task_id="branch_create_partition",
        python_callable=lambda execution_date: "create_partition_ha"
                            if execution_date.year >= 2000
                            else 'skip_create_partition',
    )

    create_partition_ha = SQLExecuteQueryOperator(
        task_id="create_partition_ha", conn_id="postgres", sql="/earthquakes/create_partition.sql"
    )

    skip_create_partition = EmptyOperator(task_id="skip_create_partition")

    # Из временной таблицы в историческую ненормализированную таблицу с партициями по годам
    sa_to_ha = SQLExecuteQueryOperator(
        task_id="sa_to_ha",
        conn_id="postgres",
        sql="/earthquakes/sa_to_ha.sql",
    )

    # Загружать ли данные в dwh (а не только в историческую таблицу)
    is_needed_dwh = ShortCircuitOperator(
        task_id="is_needed_dwh",
        python_callable=lambda execution_date: execution_date >= dwh_start_date,
    )

    # Из временной таблицы в нормализированный слой с индексами и ключами
    sa_to_dwh = SQLExecuteQueryOperator(
        task_id="sa_to_dwh",
        conn_id="postgres",
        sql="/earthquakes/sa_to_dwh.sql",
    )

    _ = start >> check_no_data >> branch_partition_ha
    _ = branch_partition_ha >> create_partition_ha >> parsed_to_sa
    _ = branch_partition_ha >> skip_create_partition >> parsed_to_sa
    _ = parsed_to_sa >> sa_to_ha >> end
    _ = parsed_to_sa >> is_needed_dwh >> sa_to_dwh >> end


earthquakes()
