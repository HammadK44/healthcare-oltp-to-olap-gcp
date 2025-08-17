CREATE OR REPLACE TABLE `healthcare-oltp-olap.healthcare_analytics.dim_source` AS
SELECT
  TO_HEX(SHA256(source)) AS source_key,
  source
FROM (
  SELECT DISTINCT source
  FROM `healthcare-oltp-olap.healthcare_analytics.fact_vitals`
);
