from datetime import datetime, timedelta
import os
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator


default_args = {
    'owner': os.getenv('AIRFLOW_OWNER', 'berkay'),
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def extract_market_data():
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    extracted_data = []

    for symbol in symbols:
        url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
        response = requests.get(url)
        if response.status_code == 200:
            extracted_data.append(response.json())
        else:
            raise Exception(f"Binance API failed for {symbol}: {response.text}")
    
    return extracted_data

def load_raw_data(**kwargs):
    ti = kwargs['ti']
    market_data = ti.xcom_pull(task_ids='extract_market_data')

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()   

    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS src_market_data (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                price_usd TEXT,
                high_price TEXT,
                low_price TEXT,
                volume_24h TEXT,
                extracted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        for asset in market_data:
            insert_query = """
                INSERT INTO src_market_data (symbol, price_usd, high_price, low_price, volume_24h)
                VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (
                asset['symbol'],
                asset['lastPrice'],
                asset['highPrice'],
                asset['lowPrice'],
                asset['volume']
            ))
        
        conn.commit()
        conn.close()

with DAG(
    'market_telemetry_ingestion',
    default_args=default_args,
    description='Automated ingestion pipeline extracting robust real-time Binance market metrics',
    schedule_interval=timedelta(minutes=5),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_market_data',
        python_callable=extract_market_data,
    )

    load_task = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_raw_data,
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/market_transformations && dbt run'
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/market_transformations && dbt test'
    )
    
    extract_task >> load_task >> dbt_run_task >> dbt_test_task