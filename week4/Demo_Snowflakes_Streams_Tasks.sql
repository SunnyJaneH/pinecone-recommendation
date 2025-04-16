DROP DATABASE Demo_DB1;
CREATE DATABASE IF NOT EXISTS Demo_DB1;
USE Demo_DB1;

CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;

CREATE OR REPLACE TABLE raw_data.sales_orders (OrderID INT PRIMARY KEY,ProductName VARCHAR(100), SalesAmount FLOAT,OrderDate VARCHAR(50),CustomerID VARCHAR(50));

-- Create a stream that tracks changes (inserts, updates, deletes) in the raw table.
CREATE OR REPLACE STREAM sales_orders_stream 
ON TABLE raw_data.sales_orders;

SELECT * FROM sales_orders_stream;

-- Insert some data to populate the raw table and demonstrate changes tracked by the stream
INSERT INTO raw_data.sales_orders (OrderID, ProductName, SalesAmount, OrderDate, CustomerID) VALUES 
(1, 'Laptop', 1500.99, '2023-01-10', 'C001'),
(2, 'Smartphone', 800.50, '2023-01-12', 'C002'),
(3, 'Tablet', 450.00, '2023-01-14', 'C003'),
(4, 'Headphones', 199.99, '2023-01-15', 'C004'),
(5, 'Smartwatch', 299.99, '2023-01-18', 'C005'),
(6, 'Monitor', 300.50, '2023-01-20', 'C006'),
(7, 'Keyboard', 50.00, '2023-01-21', 'C007'),
(8, 'Mouse', 25.00, '2023-01-22', 'C008'),
(9, 'Printer', 150.75, '2023-01-25', 'C009'),
(10, 'Speakers', 120.00, '2023-01-28', 'C010'),
(11, 'Router', 100.25, '2023-01-30', 'C011'),
(12, 'Camera', 600.00, '2023-02-01', 'C012'),
(13, 'Projector', 900.00, '2023-02-03', 'C013'),
(14, 'External Hard Drive', 200.50, '2023-02-05', 'C014'),
(15, 'Flash Drive', 15.99, '2023-02-07', 'C015'),
(16, 'Charger', 20.99, '2023-02-09', 'C016'),
(17, 'Memory Card', 30.49, '2023-02-10', 'C017'),
(18, 'Webcam', 75.99, '2023-02-12', 'C018'),
(19, 'Gaming Console', 500.00, '2023-02-15', 'C019'),
(20, 'VR Headset', 650.00, '2023-02-17', 'C020');

-- Check if the stream captures the inserted data:
SELECT * FROM sales_orders_stream;

-- Create a table where the cleaned and transformed data will be stored.
CREATE OR REPLACE TABLE processed_data.cleaned_sales_orders (
    OrderID INT,
    ProductName VARCHAR(100),
    SalesAmount FLOAT,
    OrderDate DATE,
    CustomerID VARCHAR(50)
);

-- Create a task to automatically process new records or updates from the sales_orders_stream and insert them into the cleaned_sales_orders table.

CREATE OR REPLACE TASK process_sales_orders_task
WAREHOUSE = 'DEMO_WH'
SCHEDULE = 'USING CRON 55 * * * * UTC'  -- Runs at the 55th minute of every hour
AS
INSERT INTO processed_data.cleaned_sales_orders (OrderID, ProductName, SalesAmount, OrderDate, CustomerID)
SELECT 
    OrderID,
    CASE 
        WHEN ProductName LIKE 'Laptop%' THEN 'Laptop'
        WHEN ProductName LIKE 'Smartphone%' THEN 'Smartphone'
        WHEN ProductName LIKE 'Tablet%' THEN 'Tablet'
        ELSE ProductName
    END AS ProductName,
    COALESCE(SalesAmount, 0) AS SalesAmount,
    TRY_TO_DATE(OrderDate, 'YYYY-MM-DD') AS OrderDate,
    UPPER(CustomerID) AS CustomerID
FROM 
    sales_orders_stream
WHERE METADATA$ACTION = 'INSERT' ; -- Process only inserts for this task.

-- Check the task initial status
SHOW TASKS; -- state is suspended right now

-- To enable tasks
ALTER TASK process_sales_orders_task RESUME;

-- verify the task's status by running
SHOW TASKS; -- state is started now

-- If you want to manually trigger the task instead of waiting for the cron schedule, you can execute:
EXECUTE TASK process_sales_orders_task;

-- verify the task's status by running
SHOW TASKS;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY ;

SELECT * FROM processed_data.cleaned_sales_orders;


-- Snowflake also offers dynamic tables, which automatically refresh the data based on a defined schedule, without needing to manage tasks and streams explicitly.

-- Here’s how you can use a dynamic table to process data from sales_orders directly:
CREATE OR REPLACE DYNAMIC TABLE processed_data.dynamic_cleaned_sales_orders
WAREHOUSE = 'DEMO_WH'
LAG = '1 MINUTE'  -- Automatically updates every minute
AS
SELECT 
    OrderID,
    CASE 
        WHEN ProductName LIKE 'Laptop%' THEN 'Laptop'
        WHEN ProductName LIKE 'Smartphone%' THEN 'Smartphone'
        WHEN ProductName LIKE 'Tablet%' THEN 'Tablet'
        ELSE ProductName
    END AS ProductName,
    COALESCE(SalesAmount, 0) AS SalesAmount,
    TRY_TO_DATE(OrderDate, 'YYYY-MM-DD') AS OrderDate,
    UPPER(CustomerID) AS CustomerID,
    CURRENT_TIMESTAMP AS Processed_date
FROM 
    raw_data.sales_orders;

SELECT * FROM processed_data.dynamic_cleaned_sales_orders;

-- Insert new data
INSERT INTO raw_data.sales_orders (OrderID, ProductName, SalesAmount, OrderDate, CustomerID)
VALUES (21, 'Laptop', 1700.50, '2023-02-20', 'C021');

-- Update existing data
UPDATE raw_data.sales_orders
SET SalesAmount = 1800.99
WHERE OrderID = 1;

-- Delete data
DELETE FROM raw_data.sales_orders
WHERE OrderID = 10;

SELECT * FROM processed_data.dynamic_cleaned_sales_orders;
SELECT * FROM processed_data.cleaned_sales_orders;



