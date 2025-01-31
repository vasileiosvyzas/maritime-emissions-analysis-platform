# Synthetic Datasets Documentation

# Overview
This documentation describes two synthetic datasets created to complement the CO2 emissions datasets obtained from the <a href="https://mrv.emsa.europa.eu/#public/eumrv" target="_blank">THETIS-MRV</a> system by EMSA.
1. Ship Technical Specifications Dataset
2. Ship Voyages Dataset (Yearly Aggregated)


# Dataset 1: Ship Technical Specifications
## Description
This dataset describes the technical specifications of ships or <a href="https://www.marineinsight.com/naval-architecture/what-are-vessels-particulars/#:~:text=This%20is%20known%20as%20the,of%20the%20detailed%20contract%20design." target="_blank">vessel particulars</a>. The dataset was generated to simulate realistic vessel characteristics that would typically be found in commercial shipping operations.

## Data Generation Methodology
A dataset of the technical specifications of real vessels was obtained from wikipedia articles. This dataset was filtered and processed for completeness and it was used as the ground truth as it contained the real values of particular vessels. 

Then, this ground truth sample was used with the GaussianCopulaSynthesizer from the **Synthetic Data Vault** Python library which uses machine learning to learn patterns from the real data and use them to create synthetic data.

## Schema
- imo_number (int): Unique identifier for each vessel
- gross_tonnage (int): Vessel's gross tonnage
- dwt (tonnes) (float): Vessels deadweight is the maximum weight the ship can carry effectively
- length (m) (float): Length of the vessel
- beam (m) (float): Width of the vessel
- built_year (int): Year the vessel was built
- synthetic (boolean): Identifier to separate the real from the synthetic data

## Data Quality and Limitations
- Patterns and relationships in the data are simulated
- May not capture all edge cases found in real vessel specifications