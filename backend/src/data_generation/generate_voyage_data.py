import os
import boto3

import pandas as pd
import numpy as np
import awswrangler as wr

from dotenv import load_dotenv
load_dotenv()

class VoyageDataGenerator:
    def __init__(self):
        self.vessel_types = {
            'Container ship': {
                'port_calls': (150, 250),
                'eu_port_ratio': (0.6, 0.7),
                'total_distance': (70000, 120000),
                'trip_distance': (800, 4000),
                'laden_ratio': (0.90, 0.95),
                'sea_days': (280, 320),
                'port_stay': (1, 3)
            },
            'Bulk carrier': {
                'port_calls': (30, 50),
                'eu_port_ratio': (0.4, 0.6),
                'total_distance': (45000, 80000),
                'trip_distance': (2000, 8000),
                'laden_ratio': (0.45, 0.55),
                'sea_days': (250, 300),
                'port_stay': (3, 7)
            },
            'Oil tanker': {
                'port_calls': (25, 40),
                'eu_port_ratio': (0.3, 0.5),
                'total_distance': (40000, 70000),
                'trip_distance': (3000, 10000),
                'laden_ratio': (0.45, 0.55),
                'sea_days': (260, 310),
                'port_stay': (2, 4)
            },
            'Chemical tanker': {
                'port_calls': (40, 60),
                'eu_port_ratio': (0.5, 0.7),
                'total_distance': (45000, 75000),
                'trip_distance': (1000, 5000),
                'laden_ratio': (0.50, 0.60),
                'sea_days': (270, 310),
                'port_stay': (2, 4)
            },
            'Gas carrier': {
                'port_calls': (30, 45),
                'eu_port_ratio': (0.4, 0.6),
                'total_distance': (50000, 80000),
                'trip_distance': (2000, 6000),
                'laden_ratio': (0.48, 0.55),
                'sea_days': (265, 310),
                'port_stay': (2, 4)
            },
            'LNG carrier': {
                'port_calls': (25, 40),
                'eu_port_ratio': (0.3, 0.5),
                'total_distance': (60000, 90000),
                'trip_distance': (3000, 8000),
                'laden_ratio': (0.48, 0.55),
                'sea_days': (270, 315),
                'port_stay': (1, 3)
            },
            'General cargo ship': {
                'port_calls': (60, 100),
                'eu_port_ratio': (0.5, 0.7),
                'total_distance': (40000, 70000),
                'trip_distance': (500, 3000),
                'laden_ratio': (0.70, 0.85),
                'sea_days': (260, 300),
                'port_stay': (2, 4)
            },
            'Ro-ro ship': {
                'port_calls': (100, 150),
                'eu_port_ratio': (0.6, 0.8),
                'total_distance': (40000, 70000),
                'trip_distance': (300, 2000),
                'laden_ratio': (0.85, 0.95),
                'sea_days': (270, 310),
                'port_stay': (1, 2)
            },
            'Vehicle carrier': {
                'port_calls': (80, 120),
                'eu_port_ratio': (0.5, 0.7),
                'total_distance': (50000, 90000),
                'trip_distance': (1000, 3000),
                'laden_ratio': (0.85, 0.90),
                'sea_days': (270, 310),
                'port_stay': (1, 2)
            },
            'Passenger ship': {
                'port_calls': (200, 300),
                'eu_port_ratio': (0.7, 0.9),
                'total_distance': (30000, 50000),
                'trip_distance': (100, 500),
                'laden_ratio': (0.95, 0.98),
                'sea_days': (330, 350),
                'port_stay': (0.3, 0.5)  # In days (8-12 hours)
            },
            'Ro-pax ship': {
                'port_calls': (300, 400),
                'eu_port_ratio': (0.8, 0.95),
                'total_distance': (25000, 40000),
                'trip_distance': (50, 300),
                'laden_ratio': (0.95, 0.98),
                'sea_days': (340, 355),
                'port_stay': (0.2, 0.4)  # In days (4-10 hours)
            },
            'Refrigerated cargo carrier': {
                'port_calls': (50, 80),
                'eu_port_ratio': (0.4, 0.6),
                'total_distance': (45000, 75000),
                'trip_distance': (1000, 4000),
                'laden_ratio': (0.80, 0.90),
                'sea_days': (270, 310),
                'port_stay': (2, 4)
            },
            'Container/ro-ro cargo ship': {
                'port_calls': (100, 160),
                'eu_port_ratio': (0.5, 0.7),
                'total_distance': (50000, 80000),
                'trip_distance': (500, 2500),
                'laden_ratio': (0.85, 0.92),
                'sea_days': (270, 310),
                'port_stay': (1, 3)
            }
        }
    
    
    def generate_vessel_data(self, imo_number, vessel_type, year):
        """Generate synthetic voyage data for a single vessel."""
        specs = self.vessel_types[vessel_type]
        
        # Base calculations
        port_calls = int(np.random.uniform(*specs['port_calls']))
        eu_ratio = np.random.uniform(*specs['eu_port_ratio'])
        
        # Calculate port calls maintaining relationship
        eu_port_calls = int(port_calls * eu_ratio)
        non_eu_port_calls = port_calls - eu_port_calls
        
        # Calculate distances
        avg_trip_distance = np.random.uniform(*specs['trip_distance'])
        total_distance = port_calls * avg_trip_distance
        
        # Ensure total distance falls within realistic range
        if total_distance > specs['total_distance'][1]:
            total_distance = specs['total_distance'][1]
            avg_trip_distance = total_distance / port_calls
        
        # Calculate EU waters distance based on port ratio
        # Add some randomness but maintain relationship
        eu_distance_ratio = eu_ratio * np.random.uniform(0.9, 1.1)
        eu_distance = total_distance * eu_distance_ratio
        non_eu_distance = total_distance - eu_distance
        
        # Calculate laden/ballast voyages
        laden_ratio = np.random.uniform(*specs['laden_ratio'])
        laden_voyages = int(port_calls * laden_ratio)
        ballast_voyages = port_calls - laden_voyages
        
        # Calculate time distributions
        sea_days = np.random.uniform(*specs['sea_days'])
        avg_port_stay = np.random.uniform(*specs['port_stay'])
        total_port_days = port_calls * avg_port_stay
        
        route_type = np.random.choice(['Short-haul', 'Long-haul', 'Transoceanic'])
        weather_conditions = np.random.choice(['Calm', 'Moderate', 'Rough'])
        
        
        return {
            'imo_number': imo_number,
            'reporting_period': year,
            'ship_type': vessel_type,
            'Total_Port_Calls': port_calls,
            'EU_Port_Calls': eu_port_calls,
            'Non_EU_Port_Calls': non_eu_port_calls,
            'Total_Distance': round(total_distance, 2),
            'Distance_EU_Waters': round(eu_distance, 2),
            'Distance_Non_EU_Waters': round(non_eu_distance, 2),
            'Average_Trip_Distance': round(avg_trip_distance, 2),
            'Laden_Voyages': laden_voyages,
            'Ballast_Voyages': ballast_voyages,
            'Days_At_Sea': round(sea_days, 2),
            'Average_Port_Stay': round(avg_port_stay, 2),
            'Route_Type': route_type,
            'Weather_Conditions': weather_conditions
        }
        
    def generate_fleet_data(self, vessel_data, years):
        """Generate synthetic voyage data for multiple vessels over multiple years."""
        all_data = []
        
        for _, vessel in vessel_data.iterrows():
            imo = vessel['imo_number']
            vessel_type = vessel['ship_type']
            
            # Only generate for supported vessel types
            if vessel_type in self.vessel_types:
                for year in years:
                    vessel_yearly_data = self.generate_vessel_data(
                        imo, vessel_type, year
                    )
                    all_data.append(vessel_yearly_data)
        
        return pd.DataFrame(all_data)
    
def fetch_ship_ids_and_types():   
    DATABASE = os.environ['DATABASE']
    TABLE = os.environ['TABLE']
    OUTPUT_LOCATION = os.environ['QUERY_LOCATION']
    my_session = boto3.session.Session(
        region_name=os.environ['REGION'], 
        aws_access_key_id=os.environ['ACCESS_KEY'], 
        aws_secret_access_key=os.environ['SECRET_KEY']
    )
    query = f"""
        WITH latest_versions AS (
            SELECT CAST(year AS INTEGER) AS year, MAX(CAST(version AS INTEGER)) AS latest_version
            FROM "{DATABASE}"."{TABLE}"
            GROUP BY CAST(year AS INTEGER)
        ),

        latest_data AS (
            SELECT *
            FROM "{DATABASE}"."{TABLE}" se
            JOIN latest_versions lv
            ON CAST(se.year AS INT) = lv.year
            AND CAST(se.version AS INT) = lv.latest_version
        )
        
        SELECT DISTINCT imo_number, ship_type FROM latest_data;
    """
    basic_vessels = wr.athena.read_sql_query(query, database=DATABASE, boto3_session=my_session)
    return basic_vessels


def main():
    # Initialize generator
    generator = VoyageDataGenerator()
    basic_vessels = fetch_ship_ids_and_types()

    # Generate data for 2018-2023
    years = range(2018, 2024)
    synthetic_data = generator.generate_fleet_data(basic_vessels, years)

    print("\nSample of generated data:")
    print(synthetic_data.head())

    synthetic_data.to_csv('../data/processed/ship_voyages.csv', index=False)

if __name__ == "__main__":
    main()