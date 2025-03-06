# -*- coding: utf-8 -*-
"""Assignment5_Data226.ipynb

from airflow import DAG
from airflow.decorators import task
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.model import Variable

def return_snowflake_conn():
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
  conn = hook.get_conn()
  return conn.cursor()

@task
def fetch_stock_data(symbol):
  API_Key = Variable.get("ALPHA_VANTAGE_API_KEY")
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_Key}&outputsize=compact'
  response = requests.get(url)
  data = response.json()
  return data["Time Series (Daily)"]

@task
def delete_existing_records(symbol):
  cur = return_snowflake_conn()
  try:
    cur.execute("BEGIN;")
    cur.execute(f"DELETE FROM dev.raw.stock_price WHERE symbol = '{symbol}';")
    cur.execute("COMMIT;")
  except Exception as e:
    cur.execute("ROLLBACK;")
    print(f"Error deleting records: {e}")
    raise e

@task
def insert_stock_data(symbol, data):
  cur = return_snowflake_conn()
  try:
    cur.execute("BEGIN;")
    for date, daily_info in data.items():
      sql = f"""INSERT INTO dev.raw.stock_price (date, open, high, low, close, volume, symbol)
                VALUES ('{date}', {daily_info["1. open"]}, {daily_info["2. high"]},
                        {daily_info["3. low"]}, {daily_info["4. close"]}, {daily_info["5. volume"]}, '{symbol}')
            """
      cur.execute(sql)
    cur.execute("COMMIT;")
  except Exception as e:
    cur.execute("ROLLBACK;")
    print(f"Error inserting records: {e}")
    raise e

@task
def verify_insertion(symbol):
  cur = return_snowflake_conn()
  try:
    cur.execute(f"SELECT COUNT(*) FROM dev.raw.stock_price WHERE symbol = '{symbol}';")
    record_count = cur.fetchone()[0]
    print(f"Total records in table for symbol {symbol}: {record_count}")
  except Exception as e:
    print(f"Error verifying records: {e}")
    raise e

with DAG(dag_id='stock_price_pipeline',
         start_date=datetime(2024,9,21),
         catchup=False,
         tags=['ETL'],
         schedule='30 2 * * *',
) as dag:
    symbol = "AAPL"

    data = fetch_stock_data(symbol)
    delete_task = delete_existing_records(symbol)
    insert_task = insert_new_records(symbol, data)
    verify_task = verify_insertion(symbol)

    delete_task >> insert_task >> verify_task