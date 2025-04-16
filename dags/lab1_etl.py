# In Cloud Composer, add snowflake-connector-python to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import yfinance as yf
import pandas as pd

def return_snowflake_conn():
    """
    Establish a connection to Snowflake using Airflow's connection ID.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract():
    """
    Fetch stock data for NVDA and AAPL from Yahoo Finance (last 180 days).
    """
    stocks = ["NVDA", "AAPL"]
    data = {}

    for stock in stocks:
        ticker = yf.Ticker(stock)
        df = ticker.history(period="180d")  # Fetch 180 days of historical data

        # Reset index and rename columns to match Snowflake table structure
        df.reset_index(inplace=True)
        df.rename(columns={"Date": "date", "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)

        # Ensure correct data types
        df["date"] = pd.to_datetime(df["date"]).dt.date  # Convert to date only (no time)
        df["volume"] = df["volume"].fillna(0).astype(int)  # Ensure volume is integer
        df["symbol"] = stock  # Add stock symbol column

        data[stock] = df  # Store in dictionary

    # Combine data from both stocks
    df_final = pd.concat(data.values(), ignore_index=True)
    return df_final.to_dict(orient="records")


@task
def transform(extracted_data):
    """
    Transform extracted stock data for consistency.
    """
    return {"symbol": extracted_data[0]["symbol"], "records": extracted_data}


@task
def load(transformed_data, target_table):
    """
    Load the transformed stock data into Snowflake.
    """
    cur = return_snowflake_conn()
    symbol = transformed_data["symbol"]
    records = transformed_data["records"]

    try:
        cur.execute("BEGIN;")  # Start transaction


        # Selecting the database
        cur.execute("USE DATABASE mylabdatabase")

        # Creating schema if it doesn't exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw")


        # Creating table inside the 'raw' schema
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            symbol STRING NOT NULL,
            date DATE NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume INT,
            PRIMARY KEY (symbol, date)
        )
        """
        cur.execute(create_table_query)
        cur.execute(f"DELETE FROM {target_table}")

        # Inserting new records
        insert_query = f"""
            INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for record in records:
            cur.execute(insert_query, (
                record["symbol"],
                record["date"],  # Already a string
                record["open"],
                record["high"],
                record["low"],
                record["close"],
                record["volume"]
            ))

        cur.execute("COMMIT;")  # Commit transaction
        print("Data inserted successfully!")

    except Exception as e:
        cur.execute("ROLLBACK;")  # Rollback in case of an error
        print("Error occurred:", e)
        raise e


with DAG(
    dag_id='Stock_YFinance_Snowflake',
    start_date=datetime(2025, 2, 21),
    catchup=False,
    tags=['ETL'],
    schedule_interval='0 2 * * *'  # Runs daily at 02:00 UTC
) as dag:
    target_table = "raw.stock_data"

    data = extract()
    transformed_data = transform(data)
    load(transformed_data, target_table)