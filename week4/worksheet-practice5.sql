-- Checking for duplicate records
SELECT COUNT(1)
FROM dev.analytics.session_summary;

SELECT COUNT(1)
FROM (
    SELECT DISTINCT userId, sessionId, ts, channel
    FROM dev.analytics.session_summary
);

-- Checking for the Presence of Recent Data (Freshness)
SELECT MIN(ts), MAX(ts)
FROM dev.analytics.session_summary;

-- Checking for primary key uniqueness
SELECT sessionId, COUNT(1)
FROM dev.analytics.session_summary
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

-- Check for columns with missing values
SELECT
   COUNT(CASE WHEN sessionId is NULL THEN 1 END) sessionid_null_count,
   COUNT(CASE WHEN userId is NULL THEN 1 END) userid_null_count,
   COUNT(CASE WHEN ts is NULL THEN 1 END) ts_null_count,
   COUNT(CASE WHEN channel is NULL THEN 1 END) channel_null_count
FROM dev.analytics.session_summary;
