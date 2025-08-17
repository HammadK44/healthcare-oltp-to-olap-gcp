CREATE OR REPLACE VIEW `healthcare-oltp-olap.monitoring.scheduler_executions_results_vw` AS
SELECT
  timestamp,
  resource.labels.job_id AS job_name,
  SAFE_CAST(httpRequest.status AS INT64) AS http_status,
  SAFE_CAST(httpRequest.status AS INT64) BETWEEN 200 AND 299 AS is_success,
  httpRequest.requestUrl AS target_url,
  severity,
  _TABLE_SUFFIX AS day_partition
FROM `healthcare-oltp-olap.monitoring.cloudscheduler_googleapis_com_executions_*`
WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
  AND SAFE_CAST(httpRequest.status AS INT64) IS NOT NULL
ORDER BY timestamp DESC;
