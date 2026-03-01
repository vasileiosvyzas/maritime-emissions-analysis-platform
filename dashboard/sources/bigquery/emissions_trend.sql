WITH latest_versions AS (
  SELECT *
  FROM shipemissionstrackerid.mrp_gold.fact_mrv_annual_report
  QUALIFY ROW_NUMBER() OVER(PARTITION BY reporting_period ORDER BY CAST(version AS INT64) DESC) = 1
)

SELECT 
  reporting_period, 
  SUM(total_co__emissions__m_tonnes_) AS total_emissions
FROM latest_versions
GROUP BY reporting_period
ORDER BY reporting_period
