CREATE OR REPLACE VIEW `healthcare-oltp-olap.monitoring.scheduler_executions_last_status_vw` AS
SELECT
  timestamp AS last_event_ts,
  job_name,
  http_status AS last_http_status,
  is_success AS last_is_success,
  target_url AS last_target_url,
  severity AS last_severity
FROM `healthcare-oltp-olap.monitoring.scheduler_executions_results_vw`
QUALIFY ROW_NUMBER() OVER (PARTITION BY job_name ORDER BY timestamp DESC) = 1
ORDER BY last_event_ts DESC;
