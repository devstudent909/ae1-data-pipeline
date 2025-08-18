CREATE OR REPLACE TABLE `upheld-caldron-468622-n6.nasa_silver.nasa_staging`
PARTITION BY start_date
CLUSTER BY messageType AS
SELECT
  messageType,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', REGEXP_REPLACE(messageIssueTime, r'Z$', '+00:00')) AS messageIssueTime,
  DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S%z', REGEXP_REPLACE(messageIssueTime, r'Z$', '+00:00'))) AS start_date,
  messageBody
FROM `upheld-caldron-468622-n6.nasa_raw.nasa_staging`;
