CREATE OR REPLACE TABLE `healthcare-oltp-olap.healthcare_analytics.dim_patient` AS
SELECT
  TO_HEX(SHA256(patient_id)) AS patient_key,
  patient_id,
  MIN(DATE(effective_ts)) AS first_seen_date,
  MAX(DATE(effective_ts)) AS last_seen_date,
  COUNT(*) AS measurement_count
FROM `healthcare-oltp-olap.healthcare_analytics.fact_vitals`
GROUP BY patient_id;
