from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
     - Create a view with training related columns
     - Create the forecasting function directly here
    """
    create_view_sql = f"""
    CREATE OR REPLACE VIEW {train_view} AS 
    SELECT DATE, CLOSE_PRICE, SYMBOL FROM {train_input_table};
    """

    create_forecast_function_sql = f"""
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => TABLE({train_view}),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE_PRICE',
        CONFIG_OBJECT => {{'ON_ERROR': 'SKIP'}}
    );
    """

    try:
        cur.execute(create_view_sql)
        cur.execute(create_forecast_function_sql)
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(e)
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
     - Generate predictions and store to table
     - Union predictions with historical data
    """
    make_prediction_sql = f"""
    BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;
    """

    create_final_table_sql = f"""
    CREATE OR REPLACE TABLE {final_table} AS
    SELECT SYMBOL, DATE, CLOSE_PRICE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM {train_input_table}
    UNION ALL
    SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM {forecast_table};
    """

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(e)
        raise

with DAG(
    dag_id='TrainPredict',
    start_date=datetime(2025, 2, 21),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule='30 2 * * *'
) as dag:

    train_input_table = "dev.raw.apple_price"
    train_view = "dev.adhoc.stock_data_view"
    forecast_table = "dev.adhoc.stock_data_forecast"
    forecast_function_name = "dev.analytics.predict_stock_price"
    final_table = "dev.analytics.market_data"

    cur = return_snowflake_conn()

    train(cur, train_input_table, train_view, forecast_function_name)
    predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)