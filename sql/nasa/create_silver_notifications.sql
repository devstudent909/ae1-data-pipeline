-- create_silver_notifications.sql
-- Purpose: Create NASA Silver layer table from Staging dataset
-- Runs in BigQuery

-- creates TABLE: project.dataset.table
CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.nasa_silver.notifications`
PARTITION BY DATE(messageIssueTime_ts)
CLUSTER BY messageType AS
SELECT
  messageType,
  SAFE_CAST(messageIssueTime AS TIMESTAMP) AS messageIssueTime_ts,
  messageBody
FROM `upheld-caldron-468622-n6.nasa_raw.nasa_staging`
WHERE SAFE_CAST(messageIssueTime AS TIMESTAMP) IS NOT NULL;
