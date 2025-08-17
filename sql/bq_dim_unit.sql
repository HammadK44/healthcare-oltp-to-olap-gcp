CREATE OR REPLACE TABLE `healthcare-oltp-olap.healthcare_analytics.dim_unit` AS
SELECT
  TO_HEX(SHA256(unit)) AS unit_key,
  unit
FROM (
  SELECT DISTINCT unit
  FROM `healthcare-oltp-olap.healthcare_analytics.fact_vitals`
);
