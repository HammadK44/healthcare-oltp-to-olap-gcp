CREATE OR REPLACE TABLE `healthcare-oltp-olap.healthcare_analytics.fact_vitals_star`
PARTITION BY DATE(effective_ts)
CLUSTER BY patient_key, code_key AS
SELECT
  `healthcare-oltp-olap.healthcare_analytics.dim_patient`.patient_key,
  `healthcare-oltp-olap.healthcare_analytics.dim_code`.code_key,
  `healthcare-oltp-olap.healthcare_analytics.dim_unit`.unit_key,
  `healthcare-oltp-olap.healthcare_analytics.dim_source`.source_key,
  DATE(`healthcare-oltp-olap.healthcare_analytics.fact_vitals`.effective_ts) AS date_key,
  `healthcare-oltp-olap.healthcare_analytics.fact_vitals`.event_id,
  `healthcare-oltp-olap.healthcare_analytics.fact_vitals`.value_num AS measure_value,
  `healthcare-oltp-olap.healthcare_analytics.fact_vitals`.effective_ts
FROM `healthcare-oltp-olap.healthcare_analytics.fact_vitals`
JOIN `healthcare-oltp-olap.healthcare_analytics.dim_patient` USING (patient_id)
JOIN `healthcare-oltp-olap.healthcare_analytics.dim_code`    USING (loinc_code)
JOIN `healthcare-oltp-olap.healthcare_analytics.dim_unit`    USING (unit)
JOIN `healthcare-oltp-olap.healthcare_analytics.dim_source`  USING (source);
