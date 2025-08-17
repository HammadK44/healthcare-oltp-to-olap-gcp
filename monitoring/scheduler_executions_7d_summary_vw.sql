CREATE OR REPLACE VIEW `healthcare-oltp-olap.monitoring.scheduler_executions_7d_summary_vw` AS
SELECT
  job_name,
  COUNT(*) AS runs_7d,
  SUM(CASE WHEN is_success THEN 1 ELSE 0 END) AS success_7d,
  SAFE_DIVIDE(SUM(CASE WHEN is_success THEN 1 ELSE 0 END), COUNT(*)) AS success_rate_7d
FROM `healthcare-oltp-olap.monitoring.scheduler_executions_results_vw`
GROUP BY job_name
ORDER BY success_rate_7d DESC, runs_7d DESC;
