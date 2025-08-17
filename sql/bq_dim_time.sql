CREATE OR REPLACE TABLE `healthcare-oltp-olap.healthcare_analytics.dim_time` AS
SELECT
  date_key,
  CAST(FORMAT_DATE('%Y%m%d', date_key) AS INT64) AS date_id,
  EXTRACT(YEAR FROM date_key) AS year,
  EXTRACT(QUARTER FROM date_key) AS quarter,
  EXTRACT(MONTH FROM date_key) AS month,
  FORMAT_DATE('%B', date_key) AS month_name,
  EXTRACT(ISOWEEK FROM date_key) AS iso_week,
  EXTRACT(DAY FROM date_key) AS day_of_month,
  EXTRACT(DAYOFWEEK FROM date_key) AS day_of_week,
  FORMAT_DATE('%A', date_key) AS day_name,
  IF(EXTRACT(DAYOFWEEK FROM date_key) IN (1,7), TRUE, FALSE) AS is_weekend
FROM UNNEST(
  GENERATE_DATE_ARRAY(
    (SELECT DATE(MIN(effective_ts)) FROM `healthcare-oltp-olap.healthcare_analytics.fact_vitals`),
    (SELECT DATE(MAX(effective_ts)) FROM `healthcare-oltp-olap.healthcare_analytics.fact_vitals`)
  )
) AS date_key;