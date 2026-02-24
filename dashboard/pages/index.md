---
title: Welcome to Evidence
---

<Details title='How to edit this page'>

  This page can be found in your project at `/pages/index.md`. Make a change to the markdown file and save it to see the change take effect in your browser.
</Details>

```big_values
    SELECT *
    FROM bigquery.fact_report
```

```num_ships
  SELECT COUNT(DISTINCT imo_number) AS num_of_ships
  FROM bigquery.dim_ship
  WHERE latest_reporting_period = (SELECT MAX(latest_reporting_period) FROM bigquery.dim_ship)
```

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
/>

## What's Next?
- [Connect your data sources](settings)
- Edit/add markdown files in the `pages` folder
- Deploy your project with [Evidence Cloud](https://evidence.dev/cloud)

## Get Support
- Message us on [Slack](https://slack.evidence.dev/)
- Read the [Docs](https://docs.evidence.dev/)
- Open an issue on [Github](https://github.com/evidence-dev/evidence)
