CREATE OR REPLACE VIEW `healthcare-oltp-olap.monitoring.scheduler_executions_errors_vw` AS
SELECT
  timestamp,
  job_name,
  http_status,
  target_url,
  severity
FROM `healthcare-oltp-olap.monitoring.scheduler_executions_results_vw`
WHERE NOT is_success
ORDER BY timestamp DESC;
