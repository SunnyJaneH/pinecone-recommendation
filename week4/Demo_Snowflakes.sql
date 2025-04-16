DROP DATABASE IF EXISTS DEMO_DB;

CREATE or REPLACE DATABASE DEMO_DB;

CREATE SCHEMA raw_data;

CREATE STAGE stage_company_metadata
	URL = 's3://sfquickstarts/zero_to_snowflake/cybersyn-consumer-company-metadata-csv/' 
	DIRECTORY = ( ENABLE = true );
    
LIST @stage_company_metadata;

CREATE OR REPLACE TABLE raw_data.company_metadata
(cybersyn_company_id string,
company_name string,
permid_security_id string,
primary_ticker string,
security_name string,
asset_class string,
primary_exchange_code string,
primary_exchange_name string,
security_status string,
global_tickers variant,
exchange_code variant,
permid_quote_id variant);

CREATE OR REPLACE FILE FORMAT csv_company_metadata
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'  
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n'  
    SKIP_HEADER = 1  
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'  
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
    ESCAPE = 'NONE'  
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('')
    COMMENT = 'File format for ingesting data for zero to snowflake';

SHOW FILE FORMATS IN DATABASE COMPANY_DB;

COPY INTO raw_data.company_metadata FROM @stage_company_metadata file_format=csv_company_metadata PATTERN = '.*csv.*' ON_ERROR = 'CONTINUE';


CREATE SCHEMA dev_environment;

CREATE TABLE dev_environment.company_metadata_dev CLONE raw_data.company_metadata;

SELECT * FROM dev_environment.company_metadata_dev LIMIT 10;

SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    ID,
    CLONE_GROUP_ID,
    ACTIVE_BYTES
FROM 
    INFORMATION_SCHEMA.TABLE_STORAGE_METRICS WHERE TABLE_CATALOG='DEMO_DB';

CREATE SCHEMA test_environment;

CREATE TABLE test_environment.company_metadata_test CLONE raw_data.company_metadata;

select * from raw_data.company_metadata;

DROP TABLE company_metadata_test;

SELECT * FROM company_metadata_test LIMIT 10;

UNDROP TABLE company_metadata_test;

UPDATE test_environment.company_metadata_test SET company_name = 'oops';

SELECT *
FROM company_metadata_test;


-- Set the session variable for the query_id
SET query_id = (
  SELECT query_id
  FROM TABLE(information_schema.query_history_by_session(result_limit=>5))
  WHERE query_text LIKE 'UPDATE%'
  ORDER BY start_time DESC
  LIMIT 1
);

-- 01b6fdeb-0002-e7ab-0000-ca7f0001d3be

-- Use the session variable with the identifier syntax (e.g., $query_id)
CREATE OR REPLACE TABLE test_environment.company_metadata_test AS
SELECT *
FROM test_environment.company_metadata_test
BEFORE (STATEMENT => $query_id);

-- Verify the company names have been restored
SELECT *
FROM test_environment.company_metadata_test;
