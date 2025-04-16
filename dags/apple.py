from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests

# Fetch configuration information from Airflow variables
vantage_api_key = Variable.get('vantage_api_key')
symbol = Variable.get('symbol')
url = Variable.get('url')

# Function to return Snowflake cursor
def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    # Establish the connection and return the cursor
    conn = hook.get_conn()
    return conn.cursor()

# Extraction task: Download data from the given URL
@task
def extract(url):
    r = requests.get(url)
    data = r.json()
    results = []   # Empty list to store stock information for the past 90 days (open price, high price, low price, close price, volume)
    for d in list(data["Time Series (Daily)"].keys())[:90]:   # Date format is "YYYY-MM-DD"
        stock_info = data["Time Series (Daily)"][d]
        stock_info["date"] = d  # Add the date to the stock information
        results.append(stock_info)
    print("Extract task executed with stock data:", results)
    return results

# Transformation task: Add a 'symbol' column to each row of data
@task
def transform(stock_data):
    for row in stock_data:
        row["symbol"] = symbol
        row["open_price"] = row.pop("1. open")
        row["high_price"] = row.pop("2. high")
        row["low_price"] = row.pop("3. low")
        row["close_price"] = row.pop("4. close")
        row["volume"] = row.pop("5. volume")
    print("Transform task executed with data:", stock_data)
    return stock_data

# Load task: Load data into the Snowflake table
@task
def load(table, stock_data):
    cursor = return_snowflake_conn()  # Use the function to get the Snowflake cursor
    target_table = table
    try:
        print("Begin transaction")
        cursor.execute("BEGIN;")
        
        print("Using database and schema")
        cursor.execute("USE DATABASE dev;")
        cursor.execute("USE SCHEMA raw;")
        
        print("Creating table if it does not already exist")
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open_price DECIMAL(10, 4),
            close_price DECIMAL(10, 4),
            high_price DECIMAL(10, 4),
            low_price DECIMAL(10, 4),
            volume BIGINT,
            PRIMARY KEY (symbol, date)
        )
        """)
        print("Table created or already exists.")
        
        print("Deleting existing data from the table")
        cursor.execute(f'DELETE FROM {target_table}')
        
        # Insert records into the table
        for record in stock_data:
            symbol = record["symbol"]
            date = record['date']
            open_price = record["open_price"]
            high_price = record["high_price"]
            low_price = record["low_price"]
            close_price = record["close_price"]
            volume = record["volume"]
            
            insert_sql = f"""
            INSERT INTO {target_table} (symbol, date, open_price, high_price, low_price, close_price, volume)
            VALUES ('{symbol}', '{date}', {open_price}, {high_price}, {low_price}, {close_price}, {volume})
            """
            print(f"Executing SQL: {insert_sql}")
            cursor.execute(insert_sql)
        
        cursor.execute("COMMIT;")
        print("Transaction committed")
        
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print("Transaction rolled back due to error:", e)
        raise e
    finally:
        cursor.close()

# Define the DAG
with DAG(
    dag_id='ApplePrice',
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ETL'],
    schedule_interval='30 2 * * *'
) as dag:

    target_table = "apple_price"
    extract_task = extract(url)
    transform_task = transform(extract_task)
    load_task = load(target_table, transform_task)

    extract_task >> transform_task >> load_task