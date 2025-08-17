CREATE OR REPLACE TABLE `healthcare-oltp-olap.healthcare_analytics.dim_code` AS
SELECT
  TO_HEX(SHA256(loinc_code)) AS code_key,
  loinc_code,
  ANY_VALUE(code_display) AS code_display
FROM `healthcare-oltp-olap.healthcare_analytics.fact_vitals`
GROUP BY loinc_code;
