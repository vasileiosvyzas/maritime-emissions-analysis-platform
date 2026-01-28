# Maritime Emissions Analysis Platform Data Model
# Overview
The model follows a **star schema design** to optimize query performance for analytical workloads. The central fact table captures CO₂, CH₄, N₂O emissions and fuel consumption data, while surrounding dimension tables provide context for analysis.

# Schema
## dim_ship
Stores the latest know attributes of each ship as published in the website. This table does not represent historical state.

**Grain**
One row per vessel (`imo_number`).

| Column Name                | Description                                  |
| -------------------------- | -------------------------------------------- |
| `imo_number` (PK)          | Unique IMO number identifying the ship       |
| `current_name`             | Latest known ship name                       |
| `current_ship_type`        | Latest known ship type                       |
| `current_home_port`        | Latest known home port                       |
| `current_port_of_registry` | Latest known port of registry                |
| `ice_class`                | Latest known ice class                       |
| `latest_reporting_period`  | Reporting year of the latest source report   |
| `latest_version`           | Version number of the latest source report   |
| `latest_generation_date`   | Publication date of the latest source report |

## dim_company
Stores the atttributes of the company owning or operating the ship as published in the website. This table does not represent historical state.

**Grain**
One row per company (`company_id`).
| Column Name       | Description        |
| ----------------- | ------------------ |
| `company_id` (PK) | Company IMO number |
| `company_name`    | Company name       |

## dim_verifier
Stores the attributes of the verifier bodies responsible for verifying and publishing the reports.

**Grain**
One row per verifier accreditation.

| Column Name                          | Description                 |
| ------------------------------------ | --------------------------- |
| `verifier_accreditation_number` (PK) | Verifier accreditation ID   |
| `verifier_name`                      | Verifier name               |
| `verifier_city`                      | City                        |
| `verifier_country`                   | Country                     |
| `verifier_nab`                       | National Accreditation Body |

## fact_mrv_annnual_report
Stores all the emission and fuel consumption data reported by each ship for each reporting year. It also includes all the historical ship attributes as published in each report.

This table is the registry of all historical records as published in the EU-MRV system.


### Keys

| Column                               | Description                                                                      |
| ------------------------------------ | -------------------------------------------------------------------------------- |
| `fact_id` (PK)                       | Deterministic surrogate key derived from (imo_number, reporting_period, version) |
| `ship_id` (FK)                       | References `DIM_SHIP.imo_number`                                                 |
| `ship_company_imo_number` (FK)       | References `DIM_COMPANY.company_id`                                              |
| `verifier_accreditation_number` (FK) | References `DIM_VERIFIER.verifier_accreditation_number`                          |

---

### Snapshot Attributes (As-Reported)

These attributes reflect the ship state *as reported* in the specific annual report.

| Column Name             | Description                                 |
| ----------------------- | ------------------------------------------- |
| `ship_name_reported`    | Ship name reported for the reporting period |
| `ship_type_reported`    | Ship type reported                          |
| `company_name_reported` | Owning company name reported                |
| `ice_class_reported`    | Ice class reported                          |

These values may differ from the current state stored in `DIM_SHIP`.

---

### Report Metadata

| Column Name        | Description                    |
| ------------------ | ------------------------------ |
| `reporting_period` | Reporting year (e.g. 2021)     |
| `version`          | Report version number          |
| `generation_date`  | Publication date of the report |

---

### Metrics

The fact table includes metrics across multiple domains:

* Fuel consumption
* CO₂, CH₄, N₂O emissions
* CO₂-equivalent emissions
* Distance and time
* Efficiency and intensity indicators

All metrics are reported **exactly as published** in the source MRV reports.

Null values may occur when metrics were not reported in earlier years.

---