CREATE TABLE `{project}.{dataset}.dim_ship` (
    imo_number INT64 NOT NULL,
    current_name STRING NOT NULL,
    current_ship_type STRING,
    current_home_port STRING,
    current_port_of_registry STRING,
    ice_class STRING,
    latest_reporting_period INT64,
    latest_version INT64,
    latest_generation_date DATE
);

CREATE TABLE `{project}.{dataset}.dim_company` (
  company_id      INT64    NOT NULL,
  company_name    STRING  NOT NULL
);

CREATE TABLE `{project}.{dataset}.dim_verifier` (
  verifier_accreditation_number  STRING  NOT NULL,
  verifier_name                  STRING  NOT NULL,
  verifier_city                  STRING,
  verifier_country               STRING,
  verifier_nab                   STRING
);

-- Logical primary key:
-- (ship_id, reporting_period, version)

-- Logical foreign keys:
-- ship_id → dim_ship.imo_number
-- ship_company_imo_number → dim_company.company_id
-- verifier_accreditation_number → dim_verifier.verifier_accreditation_number

CREATE TABLE `{project}.{dataset}.fact_mrv_annual_report` (
  -- Foreign keys (logical)
  ship_id                         INT64   NOT NULL,
  ship_company_imo_number         INT64,
  verifier_accreditation_number   STRING,

  -- Snapshot attributes (as-reported)
  ship_name_reported              STRING,
  ship_type_reported              STRING,
  company_name_reported           STRING,
  ice_class_reported              STRING,

  -- Report metadata
  reporting_period                INT64   NOT NULL,
  version                         INT64   NOT NULL,
  generation_date                 DATE    NOT NULL,

  -- =========================
  -- Metrics (as published)
  -- =========================

  technical_efficiency_type       STRING,
  technical_efficiency_value      NUMERIC,
  technical_efficiency_unit       STRING,
  doc_issue_date                  DATE,
  doc_expiry_date                 DATE,
  monitoring_methods            STRING,
  
  total_fuel_consumption__m_tonnes_ NUMERIC,
  total_fuel_benefitting_from_a_derogation_in_accordance_with_part_c__point_1_2__of_annex_ii_to_regulation__eu__2015_757__voluntary_ NUMERIC,
    fuel_consumptions_assigned_to_on_laden__m_tonnes_ NUMERIC,
    fuel_consumptions_assigned_to_cargo_heating NUMERIC,
    fuel_consumptions_assigned_to_dynamic_positioning NUMERIC,
    total_co__emissions__m_tonnes_ NUMERIC,
    co__emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction__m_tonnes_ NUMERIC,
    co__emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction__m_tonnes_ NUMERIC,
    co__emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction__m_tonnes_ NUMERIC,
    co__emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth__m_tonnes_ NUMERIC,
    co2_emissions_which_occurred_within_ports_under_a_ms_jurisdiction NUMERIC,
    co__emissions_assigned_to_on_laden__m_tonnes_ NUMERIC,
    co__emissions_assigned_to_passenger_transport__m_tonnes_ NUMERIC,
    co__emissions_assigned_to_freight_transport__m_tonnes_ NUMERIC,
    co2_emissions_to_be_reported_under_directive_2003_87_ec NUMERIC,
    total_ch4_emissions NUMERIC,
    ch4_emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction NUMERIC,
    ch4_emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction NUMERIC,
    ch4_emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction NUMERIC,
    ch4_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth NUMERIC,
    ch4_emissions_which_occurred_within_ports_under_a_ms_jurisdiction NUMERIC,
    ch4_emissions_assigned_to_on_laden NUMERIC,
    ch4_emissions_assigned_to_passenger_transport NUMERIC,
    ch4_emissions_assigned_to_freight_transport NUMERIC,
    ch4_emissions_to_be_reported_under_directive_2003_87_ec NUMERIC,
    total_n_o_emissions NUMERIC,
    n_o_emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction NUMERIC,
    n_o_emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction NUMERIC,
    n_o_emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction NUMERIC,
    n_o_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth NUMERIC,
    n_o_emissions_which_occurred_within_ports_under_a_ms_jurisdiction NUMERIC,
    n_o_emissions_assigned_to_on_laden NUMERIC,
    n_o_emissions_assigned_to_passenger_transport NUMERIC,
    n_o_emissions_assigned_to_freight_transport NUMERIC,
    n_o_emissions_to_be_reported_under_directive_2003_87_ec NUMERIC,
    total_co2eq_emissions NUMERIC,
    co2eq_emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction NUMERIC,
    co2eq_emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction NUMERIC,
    co2eq_emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction NUMERIC,
    co2eq_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth NUMERIC,
    co2eq_emissions_which_occurred_within_ports_under_a_ms_jurisdiction NUMERIC,
    co2eq_emissions_assigned_to_on_laden NUMERIC,
    co2eq_emissions_assigned_to_passenger_transport NUMERIC,
    co2eq_emissions_assigned_to_freight_transport NUMERIC,
    co2eq_emissions_to_be_reported_under_directive_2003_87_ec NUMERIC,
    co2eq_emissions_benefitting_from_a_derogation_in_accordance_with_part_c__point_1_2__of_annex_ii_to_regulation__eu__2015_757__voluntary_ NUMERIC,
    distance_through_ice NUMERIC,
    time_spent_at_sea__hours_ NUMERIC,
    time_spent_at_sea_through_ice NUMERIC,
    fuel_consumption_per_distance NUMERIC,
    fuel_consumption_per_distance_on_laden_voyages__kg___n_mile_ NUMERIC,
    fuel_consumption_per_transport_work__mass_ NUMERIC,
    fuel_consumption_per_transport_work__mass__on_laden_voyages__g___m_tonnes___n_miles NUMERIC,
    fuel_consumption_per_transport_work__volume_ NUMERIC,
    fuel_consumption_per_transport_work__volume__on_laden_voyages__g___m____n_miles NUMERIC,
    fuel_consumption_per_transport_work__dwt_ NUMERIC,
    fuel_consumption_per_transport_work__dwt__on_laden_voyages__g___dwt_carried___n_miles NUMERIC,
    fuel_consumption_per_transport_work__pax_ NUMERIC,
    fuel_consumption_per_transport_work__pax__on_laden_voyages__g___pax___n_miles NUMERIC,
    fuel_consumption_per_transport_work__freight_ NUMERIC,
    fuel_consumption_per_transport_work__freight__on_laden_voyages__g___m_tonnes___n_miles NUMERIC,
    fuel_consumption_per_time_spent_at_sea NUMERIC,
    co2_emissions_per_distance NUMERIC,
    co__emissions_per_distance_on_laden_voyages__kg_co____n_mile_ NUMERIC,
    co2_emissions_per_transport_work__mass_ NUMERIC,
    co__emissions_per_transport_work__mass__on_laden_voyages__g_co____m_tonnes___n_miles NUMERIC,
    co2_emissions_per_transport_work__volume_ NUMERIC,
    co__emissions_per_transport_work__volume__on_laden_voyages__g_co____m____n_miles NUMERIC,
    co2_emissions_per_transport_work__dwt_ NUMERIC,
    co__emissions_per_transport_work__dwt__on_laden_voyages__g_co____dwt_carried___n_miles NUMERIC,
    co2_emissions_per_transport_work__pax_ NUMERIC,
    co__emissions_per_transport_work__pax__on_laden_voyages__g_co____pax___n_miles NUMERIC,
    co2_emissions_per_transport_work__freight_ NUMERIC,
    co__emissions_per_transport_work__freight__on_laden_voyages__g_co____m_tonnes___n_miles NUMERIC,
    co2_emissions_per_time_spent_at_sea NUMERIC,
    co2eq_emissions_per_distance NUMERIC,
    co2eq_emissions_per_distance_on_laden_voyages NUMERIC,
    co2eq_emissions_per_transport_work__mass_ NUMERIC,
    co2eq_emissions_per_transport_work__mass__on_laden_voyages NUMERIC,
    co2eq_emissions_per_transport_work__volume_ NUMERIC,
    co2eq_emissions_per_transport_work__volume__on_laden_voyages NUMERIC,
    co2eq_emissions_per_transport_work__dwt_ NUMERIC,
    co2eq_emissions_per_transport_work__dwt__on_laden_voyages NUMERIC,
    co2eq_emissions_per_transport_work__pax_ NUMERIC,
    co2eq_emissions_per_transport_work__pax__on_laden_voyages NUMERIC,
    co2eq_emissions_per_transport_work__freight_ NUMERIC,
    co2eq_emissions_per_transport_work__freight__on_laden_voyages NUMERIC,
    co2eq_emissions_per_time_spent_at_sea NUMERIC,
    average_density_of_the_cargo_transported__m_tonnes___m__ NUMERIC
)
PARTITION BY RANGE_BUCKET(reporting_period, GENERATE_ARRAY(2018, 2026, 1));