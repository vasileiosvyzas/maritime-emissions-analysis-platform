SELECT
  SUM(total_co2eq_emissions) AS total_co2eq_emissions,
  SUM(total_fuel_consumption__m_tonnes_) AS total_fuel_consumption,
  SUM(total_co__emissions__m_tonnes_) AS total_co__emissions__m_tonnes_,
  ROUND(SUM(total_fuel_consumption__m_tonnes_) / 1e6, 1) AS total_fuel_consumption_mt,
  ROUND(SUM(total_co2eq_emissions) / 1e6, 1)           AS total_co2eq_mt,
  ROUND(SUM(total_co__emissions__m_tonnes_) / 1e6, 1)  AS total_co2_mt
FROM (
  SELECT *
  FROM shipemissionstrackerid.mrp_gold.fact_mrv_annual_report
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ship_id
    ORDER BY reporting_period DESC, CAST(version AS INT64) DESC
  ) = 1
);
