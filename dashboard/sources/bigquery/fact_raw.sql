WITH latest_versions AS (
      SELECT 
        reporting_period AS year, 
        MAX(CAST(version AS INT64)) AS latest_version
      FROM `shipemissionstrackerid.mrp_gold.fact_mrv_annual_report`
      GROUP BY reporting_period
  ),

latest_data AS (
    SELECT *
    FROM `shipemissionstrackerid.mrp_gold.fact_mrv_annual_report` se
    JOIN latest_versions lv
    ON se.reporting_period = lv.year
    AND CAST(se.version AS INT64) = lv.latest_version
)

SELECT
  reporting_period,
  CAST(version AS INT64),
  ship_type_reported,
  total_co__emissions__m_tonnes_,
  total_fuel_consumption__m_tonnes_
FROM latest_data