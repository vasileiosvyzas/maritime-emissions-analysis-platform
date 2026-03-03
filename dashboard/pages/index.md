---
title: Maritime Emissions Dashboard
description: A dashboard to visualize maritime emissions data, including fuel consumption and CO2 emissions by ship type and over time.
---

This dashboard provides an overview of CO2 emissions from ships reporting to the European Union's MRV system. All data is for the most recent complete reporting period.

---

## At a Glance: Latest Reporting Period

```sql big_values
    SELECT *
    FROM bigquery.fact_report
```

```sql num_ships
  SELECT COUNT(DISTINCT imo_number) AS num_of_ships
  FROM bigquery.dim_ship
  WHERE latest_reporting_period = (SELECT MAX(latest_reporting_period) FROM bigquery.dim_ship)
```

<Grid cols=4>
    <BigValue
        data={num_ships}
        value="num_of_ships"
        title="Total Ships Reporting"
    />
    <BigValue
        data={big_values}
        value="total_fuel_consumption_mt"
        title="Total Fuel Consumption"
        unit="Mt"
    />
    <BigValue
        data={big_values}
        value="total_co2_mt"
        title="Total CO2 Emissions"
        unit="Mt"
        fmt="0.2f"
    />
    <BigValue
        data={big_values}
        value="total_co2eq_mt"
        title="Total CO2eq Emissions"
        unit="Mt"
        fmt="0.2fMt"
    />
</Grid>

<BigValue 
  data={num_ships} 
  value=num_of_ships
/>

<BigValue 
  data={big_values} 
  value=total_fuel_consumption_mt
/>
<BigValue 
  data={big_values} 
  value=total_co2eq_mt
/>
<BigValue 
  data={big_values} 
  value=total_co2_mt
/>

```sql emissions_by_year
    SELECT *
    FROM bigquery.emissions_trend
```



<LineChart 
    data={emissions_by_year}
    x=reporting_period
    y=total_emissions 
    title="Total CO2 Emissions Over Time"
    subtitle="Includes all ship types"
/>


```sql ships_by_type
    select current_ship_type, count(*) as num_ships
    from bigquery.dim_ship
    group by current_ship_type
    order by num_ships desc
```

<BarChart
    data={ships_by_type}
    title="Ships by Type"
    x=current_ship_type
    y=num_ships
    swapXY=true
/>

```sql ships_by_type_and_emissions
    select *
    from bigquery.fact_raw
```

<BarChart 
    data={ships_by_type_and_emissions}
    title="Ships with highest CO2 emissions"
    x=ship_type_reported
    y=total_co__emissions__m_tonnes_
    series=reporting_period
    swapXY=true
/>

<ScatterPlot 
    data={ships_by_type_and_emissions}
    title="Fuel Consumption vs CO2 Emissions"
    x=total_fuel_consumption__m_tonnes_
    y=total_co__emissions__m_tonnes_
    series=reporting_period
/>

```sql fuel_by_ship_type
    select *
    from bigquery.fact_raw
    WHERE reporting_period = (SELECT MAX(reporting_period) FROM bigquery.fact_raw)
```

<BarChart 
    data={fuel_by_ship_type}
    title="Ships that consume the most fuel"
    x=ship_type_reported
    y=total_fuel_consumption__m_tonnes_
    swapXY=true
/>


