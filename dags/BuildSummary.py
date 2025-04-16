from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

def return_snowflake_conn():
    """Return a Snowflake cursor connection."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def run_ctas(database, schema, table, select_sql, primary_key=None):
    """Create or replace a table in Snowflake using a CTAS (Create Table As Select) query.

    Args:
        database (str): The target database.
        schema (str): The target schema.
        table (str): The table name.
        select_sql (str): The SQL query to generate the table data.
        primary_key (str, optional): The primary key for uniqueness checks.
    """
    logging.info(f"Processing table: {table}")
    logging.info(f"Executing SQL: {select_sql}")

    cur = return_snowflake_conn()
    
    try:
        # Create a temporary table with deduplication using ROW_NUMBER()
        sql = f"""
            CREATE OR REPLACE TABLE {database}.{schema}.temp_{table} AS 
            WITH ranked_data AS (
                SELECT 
                    u.*, 
                    s.ts,
                    ROW_NUMBER() OVER (PARTITION BY u.{primary_key} ORDER BY s.ts DESC) AS row_num
                FROM dev.raw.user_session_channel u
                JOIN dev.raw.session_timestamp s ON u.sessionId = s.sessionId
            )
            SELECT * FROM ranked_data WHERE row_num = 1;
        """
        logging.info(f"Creating temp table with deduplication: {sql}")
        cur.execute(sql)

        # Check primary key uniqueness
        if primary_key:
            sql = f"""
                SELECT {primary_key}, COUNT(1) AS cnt 
                FROM {database}.{schema}.temp_{table}
                GROUP BY 1
                HAVING COUNT(1) > 1
            """
            logging.info(f"Checking primary key uniqueness: {sql}")
            cur.execute(sql)
            result = cur.fetchone()
            if result:
                raise Exception(f"Primary key uniqueness failed: {result}")
        
        # Second check for redundancy
        if primary_key is not None:
            sql = f"""
                SELECT {primary_key}, COUNT(1) AS cnt 
                FROM {database}.{schema}.temp_{table}
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 1"""
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!!!!!!!!!!!!")
                raise Exception(f"Primary key uniqueness failed: {result}")

        # Replace the main table with the cleaned data
        sql = f"CREATE OR REPLACE TABLE {database}.{schema}.{table} AS SELECT * FROM {database}.{schema}.temp_{table};"
        logging.info(f"Replacing main table: {sql}")
        cur.execute(sql)

    except Exception as e:
        logging.error(f"Error in run_ctas: {e}")
        raise
    finally:
        cur.close()  # Ensure cursor is closed

with DAG(
    dag_id='BuildSessionSummary',
    start_date=datetime(2025, 3, 23),
    catchup=False,
    tags=['ELT'],
    schedule_interval='45 2 * * *'
) as dag:

    database = "dev"
    schema = "analytics"
    table = "session_summary"
    select_sql = """SELECT u.*, s.ts
    FROM dev.raw.user_session_channel u
    JOIN dev.raw.session_timestamp s ON u.sessionId = s.sessionId
    """

    run_ctas(database, schema, table, select_sql, primary_key='sessionId')