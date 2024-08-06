from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import psycopg2
import json

default_args = {
    'owner': 'mosienko',
    'start_date': datetime(2024, 8, 6)
}

dag = DAG(
    dag_id='report_load_qty_tare_hour',
    default_args=default_args,
    schedule_interval='@daily',
    description='PG dag',
    catchup=False,
    max_active_runs=1
)


def connect_ch():
    with open('/opt/airflow/dags/credentials.json') as json_file:
        data = json.load(json_file)

    client = Client(data['clickhouse'][0]['host'],
                    user=data['clickhouse'][0]['user'],
                    password=data['clickhouse'][0]['password'],
                    port=data['clickhouse'][0]['port'],
                    verify=False,
                    settings={"numpy_columns": True, 'use_numpy': True},
                    compression=False)

    return client


def connect_pg():
    with open('/opt/airflow/dags/credentials.json') as json_file:
        data = json.load(json_file)

    client = psycopg2.connect(host=data['postgres'][0]['host'],
                                  user=data['postgres'][0]['user'],
                                  password=data['postgres'][0]['password'],
                                  port=data['postgres'][0]['port'],
                                  dbname=data['postgres'][0]['dbname'])

    return client

def main():
    main_table = "report.load_qty_tare_hour"

    sql = f'''
        insert into {main_table}
            select toStartOfHour(dt) dt_hour
                , office_id
                , uniqIf(tare_id, is_load=1) qty_load
            from tareLoad
            where dt>= now() - interval 5 hour
            group by dt_hour, office_id
    '''

    client_ch = connect_ch()
    client_ch.execute(sql)
    print(f'Запись в витрину данных {main_table} прошла успешно!')

def import_pg():
    procedure_name = "load_qty_tare_hour_importfromclick"
    main_table = "report.load_qty_tare_hour"

    sql = f'''
        select now() dt_load
            , dt_hour
            , office_id
            , qty_load
        from {main_table} final
    '''

    client_ch = connect_ch()
    df = client_ch.query_dataframe(sql)

    client_pg = connect_pg()
    cursor = client_pg.cursor()

    df = df.to_json(orient="records", date_format="iso", date_unit="s")
    cursor.execute(f"CALL sync.{procedure_name}(_src := '{df}')")
    client_pg.commit()

    print('Импорт данных прошел успешно')

    cursor.close()
    client_pg.close()


task_ch = PythonOperator(task_id='report_load_qty_tare_hour_ch', python_callable=main, dag=dag)
task_pg = PythonOperator(task_id='report_load_qty_tare_hour_pg', python_callable=import_pg, dag=dag)

task_ch >> task_pg
