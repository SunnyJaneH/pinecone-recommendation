from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Define default_args and DAG
default_args = {
    'owner': 'shagun',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'etl_flow_dag_real_time',
    default_args=default_args,
    schedule_interval=None,  # You can set the schedule as per your needs
)



# SQL command to load data from Snowflake stage into a table
sql_command1 = """
CREATE TABLE nps_table
( id NUMBER(38, 0) , created TIMESTAMP_NTZ , score NUMBER(38, 0) ); 
"""

# Create a task to load data into Snowflake
table_creation = SnowflakeOperator(
    task_id='create_table_snowflake',
    snowflake_conn_id='snowflakes_connection',  # Use your Snowflake connection ID
    sql=sql_command1,
    dag=dag,
)


# SQL command to create stage in snowflake
sql_command2 = """
CREATE STAGE GEO_S3 
	URL = 's3://s3-geospatial/readonly/' 
	DIRECTORY = ( ENABLE = true );
"""

# Create a task to load data into Snowflake
stage_creation = SnowflakeOperator(
    task_id='create_stage_snowflake',
    snowflake_conn_id='snowflakes_connection',  # Use your Snowflake connection ID
    sql=sql_command2,
    dag=dag,
)


# SQL command to create file format
sql_command3 = """
 CREATE FILE FORMAT FILE_AF
	TYPE=CSV
    SKIP_HEADER=1
    FIELD_DELIMITER=','
    TRIM_SPACE=TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    REPLACE_INVALID_CHARACTERS=TRUE
    DATE_FORMAT=AUTO
    TIME_FORMAT=AUTO
    TIMESTAMP_FORMAT=AUTO;
"""

# Create a task to load data into Snowflake
file_creation = SnowflakeOperator(
    task_id='create_file_format_snowflake',
    snowflake_conn_id='snowflakes_connection',  # Use your Snowflake connection ID
    sql=sql_command3,
    dag=dag,
)




# SQL command to create file format
sql_command4 = """
COPY INTO nps_table
	FROM @GEO_S3
FILES = ('nps.csv') 
FILE_FORMAT = FILE_AF
ON_ERROR=ABORT_STATEMENT 
"""

# Create a task to load data into Snowflake
load_into_snowflake = SnowflakeOperator(
    task_id='load_data_into_snowflake',
    snowflake_conn_id='snowflakes_connection',  # Use your Snowflake connection ID
    sql=sql_command4,
    dag=dag,
)




table_creation >> stage_creation >> file_creation >> load_into_snowflake