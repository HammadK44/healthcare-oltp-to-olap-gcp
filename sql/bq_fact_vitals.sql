CREATE OR REPLACE TABLE `healthcare-oltp-olap.healthcare_analytics.fact_vitals` AS
SELECT
  event_id,
  patient_id,
  loinc_code,
  code_display,
  value_num,
  unit,
  effective_ts,
  source,
  created_at,
  raw
FROM `healthcare-oltp-olap.healthcare_analytics.vitals_raw`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY event_id
  ORDER BY created_at DESC
) = 1;