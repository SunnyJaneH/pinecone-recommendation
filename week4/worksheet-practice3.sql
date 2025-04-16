CREATE SCHEMA IF NOT EXISTS dev.analytics;

-- To get a complete view of the session info, you have to join always
SELECT B.*, A.ts 
FROM dev.raw.session_timestamp A
JOIN dev.raw.user_session_channel B ON A.sessionid = B.sessionid
LIMIT 10;

-- You can create a table with the above SELECT for easier use
CREATE OR REPLACE TABLE dev.analytics.session_summary AS
SELECT B.*, A.ts 
FROM dev.raw.session_timestamp A
JOIN dev.raw.user_session_channel B ON A.sessionid = B.sessionid;

-- MAU computation
SELECT 
  LEFT(ts, 7) AS year_month,
  COUNT(DISTINCT userid) AS mau     -- note that there is DISTINCT
FROM dev.analytics.session_summary
GROUP BY 1 
ORDER BY 1 DESC; 

-- CTE based MAU computation
WITH tmp AS (
    SELECT B.*, A.ts
    FROM dev.raw.session_timestamp A
    JOIN dev.raw.user_session_channel B ON A.sessionid = B.sessionid
)
SELECT 
  LEFT(ts, 7) AS year_month,
  COUNT(DISTINCT userid) AS mau
FROM tmp
GROUP BY 1 
ORDER BY 1 DESC; 

-- Subquery based MAU computation
SELECT 
  LEFT(ts, 7) AS year_month,
  COUNT(DISTINCT userid) AS mau
FROM (
    SELECT B.*, A.ts
    FROM dev.raw.session_timestamp A
    JOIN dev.raw.user_session_channel B ON A.sessionid = B.sessionid
)
GROUP BY 1 
ORDER BY 1 DESC;
