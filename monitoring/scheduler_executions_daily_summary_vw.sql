CREATE OR REPLACE VIEW `healthcare-oltp-olap.monitoring.scheduler_executions_daily_summary_vw` AS
SELECT
  DATE(timestamp) AS event_date,
  job_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN is_success THEN 1 ELSE 0 END) AS success_runs,
  SUM(CASE WHEN NOT is_success THEN 1 ELSE 0 END) AS failed_runs,
  SAFE_DIVIDE(SUM(CASE WHEN is_success THEN 1 ELSE 0 END), COUNT(*)) AS success_rate
FROM `healthcare-oltp-olap.monitoring.scheduler_executions_results_vw`
GROUP BY event_date, job_name
ORDER BY event_date DESC, job_name;
