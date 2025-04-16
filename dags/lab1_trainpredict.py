from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from airflow.models import Variable

def get_snowflake_cursor():
    """
    Get a Snowflake connection cursor.
    This function establishes a new connection for each task run.
    The connection uses the pre-configured 'snowflake_conn' in Airflow.
    """
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def train(train_input_table, train_view, forecast_function_name):
    """
    Training Task:
    - Create a view that filters and prepares training data (date, close price, symbol).
    - Define and create a Snowflake ML Forecast function using the training view.
    - Display evaluation metrics to validate the model performance.
    
    Parameters:
    train_input_table: The source table containing historical stock prices.
    train_view: A temporary view created for training.
    forecast_function_name: The name of the forecast function to be created.
    """
    cur = get_snowflake_cursor()

    # SQL to create a training view that selects only necessary columns
    create_view_sql = f"""
    CREATE OR REPLACE VIEW {train_view} AS 
    SELECT DATE, CLOSE, SYMBOL FROM {train_input_table};
    """

    # SQL to create a Forecast function in Snowflake ML
    create_forecast_function_sql = f"""
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => TABLE({train_view}),
        SERIES_COLNAME => 'SYMBOL',               -- Stock ticker column
        TIMESTAMP_COLNAME => 'DATE',               -- Date column
        TARGET_COLNAME => 'CLOSE',                  -- Target variable (stock price)
        CONFIG_OBJECT => {{'ON_ERROR': 'SKIP'}}    -- Skip problematic rows
    );
    """

    try:
        # Execute SQL to create the view and forecast function
        cur.execute(create_view_sql)
        cur.execute(create_forecast_function_sql)

        # Show evaluation metrics to check training quality
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
    except Exception as e:
        print(f"Training task failed: {e}")
        raise
    finally:
        cur.close()

@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table):
    """
    Prediction Task:
    - Execute the trained forecast function to predict prices for the next 7 days.
    - Save prediction results into a table.
    - Combine historical actual prices and future predictions into a unified final table.
    
    Parameters:
    forecast_function_name: The trained forecast function name.
    train_input_table: Historical stock data table.
    forecast_table: Table to store the forecast output.
    final_table: Final table that combines actual + predicted data.
    """
    cur = get_snowflake_cursor()

    # SQL to call the forecast function and store results in a table
    make_prediction_sql = f"""
    BEGIN
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,                         -- Predict the next 7 days
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}  -- 95% confidence interval
        );
        LET x := SQLID;  -- Fetch the result identifier
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;
    """

    # SQL to create the final table combining actual historical data and forecasted future data
    create_final_table_sql = f"""
    CREATE OR REPLACE TABLE {final_table} AS
    SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM {train_input_table}
    UNION ALL
    SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM {forecast_table};
    """

    try:
        # Execute SQL to run the forecast and create final table
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
    except Exception as e:
        print(f"Prediction task failed: {e}")
        raise
    finally:
        cur.close()

# Define DAG configuration and scheduling
with DAG(
    dag_id='lab1_train_predict',
    start_date=datetime(2025, 2, 21),
    catchup=False,   # No need to backfill for past dates
    tags=['ML', 'ELT'],  # Add useful tags for easier discovery
    schedule='30 2 * * *'  # Scheduled to run daily at 2:30 AM
) as dag:

    # Table and function names used within the pipeline
    train_input_table = Variable.get("train_input_table")  # Historical stock data
    train_view = Variable.get("train_view")  # Training view
    forecast_table = Variable.get("forecast_table")  # Prediction results table
    forecast_function_name = Variable.get("forecast_function_name")  # Forecast function
    final_table = Variable.get("final_table")  # Combined actual + forecasted data table

    # Define task dependencies within the DAG
    # Step 1: Training --> Step 2: Prediction
    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)

    # Set task dependency
    train_task >> predict_task