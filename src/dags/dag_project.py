import requests
import json
from psycopg2.extras import execute_values
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

task_logger = logging.getLogger('airflow.task')

# подключение к ресурсам
api_conn = BaseHook.get_connection('api_connection')
postgres_conn = 'PG_WAREHOUSE_CONNECTION'
dwh_hook = PostgresHook(postgres_conn)

# параметры API
nickname = json.loads(api_conn.extra)['nickname']
cohort = json.loads(api_conn.extra)['cohort']
api_key = json.loads(api_conn.extra)['api_key']
base_url = api_conn.host

headers = {"X-Nickname": nickname,
           'X-Cohort': cohort,
           'X-API-KEY': api_key,
           }


def upload_deliveries(start_date, end_date):
    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    # параметры фильтрации
    start = f"{start_date} 00:00:00"
    end = f"{end_date} 23:59:59"

    # идемпотентность
    dwh_hook.run(sql=f"DELETE FROM stg.deliveries WHERE order_ts::date BETWEEN '{start_date}' AND '{end_date}'")

    # получение данных
    offset = 0
    while True:
        deliver_rep = requests.get(
            f'https://{base_url}/deliveries/?sort_field=order_ts&sort_direction=asc&from={start}&to={end}&offset={offset}',
            headers=headers).json()

        # останавливаемся, когда данные закончились
        if len(deliver_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        # запись в БД
        values = [[value for value in deliver_rep[i].values()] for i in range(len(deliver_rep))]

        sql = f"INSERT INTO stg.deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, " \
              f"sum, tip_sum) VALUES %s "
        execute_values(cursor, sql, values)

        offset += len(deliver_rep)


def upload_couriers():
    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    # идемпотентность
    dwh_hook.run(sql=f"DELETE FROM stg.couriers")

    offset = 0
    while True:
        couriers_rep = requests.get(f'https://{base_url}/couriers/?sort_field=_id&sort_direction=asc&offset={offset}',
                                    headers=headers).json()

        if len(couriers_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]

        sql = f"INSERT INTO stg.couriers (courier_id, courier_name) VALUES %s"
        execute_values(cursor, sql, values)

        offset += len(couriers_rep)


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=60)
}

dag = DAG('dag_project',
          start_date=datetime.today() - timedelta(days=7),
          catchup=True,
          schedule_interval='@daily',
          max_active_runs=1,
          default_args=default_args)

with TaskGroup(group_id='upload_stg', dag=dag) as upload_stg:
    upload_deliveries = PythonOperator(
        task_id='stg_deliveries',
        python_callable=upload_deliveries,
        op_kwargs={
            'start_date': '{{yesterday_ds}}',
            'end_date': '{{ds}}'
        },
        dag=dag
    )

    upload_couriers = PythonOperator(
        task_id='stg_couriers',
        python_callable=upload_couriers,
        dag=dag
    )

with TaskGroup(group_id='upload_dm_dds', dag=dag) as upload_dm_dds:
    dm_upd = [
        PostgresOperator(
            task_id=f"{task[0:-4]}",
            postgres_conn_id=postgres_conn,
            sql=f"{task}",
            dag=dag
        ) for task in ['stg_dds_couriers.sql', 'stg_dds_deliveries.sql', 'stg_dds_orders.sql']
    ]

dds_fct_deliveries = PostgresOperator(
    task_id='dds_fct_deliveries',
    postgres_conn_id=postgres_conn,
    sql='stg_dds_fct_deliveries.sql',
    dag=dag
)

cdm_courier_ledger = PostgresOperator(
    task_id='cdm_courier_ledger',
    postgres_conn_id=postgres_conn,
    sql='cdm_courier_ledger.sql',
    dag=dag
)

upload_stg >> upload_dm_dds >> dds_fct_deliveries >> cdm_courier_ledger
