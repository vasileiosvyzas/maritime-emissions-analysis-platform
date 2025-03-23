import pandas as pd
import numpy as np
from typing import List, Optional

from google_cloud_storage_manager import GoogleCloudStorageManager

pd.set_option('future.no_silent_downcasting', True)

class ETLPipeline():
    def __init__(self):
        self.storage_client = GoogleCloudStorageManager()

    def extract(self) -> List[pd.DataFrame]:
        """Downloads the new CO2 emission report from the bronze location in the bucket.
        It needs to identify which is the new file that has been added comparing with the "old" ones 
        that have already been processed.

        Returns:
            pd.DataFrame: the new CO2 emission report in pandas dataframe
        """
        # Go to the bucket
        # Get all the files in the bucket (list all files)
        # Figure out which files have not been processed
        # Get the files which have not been processed
        # return a dataframe of the files
        
        df_to_process = []
        blobs = self.storage_client.list_blobs(self.bucket, prefix='bronze-bucket/')
        print("Blobs:")
        for blob in blobs:
            if blob.name.endswith('.xlsx'):
                print(blob.name)
                if blob.metadata['processed_by_ETL'] == 'False':
                    df = self.storage_client.download_file_into_memory(blob_name=blob.name, bucket_layer='bronze-bucket')
                    
                    df_to_process.append(df)
        
        return df_to_process
        

    def tranform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Does all the transformations on the emission report to make it clean

        Args:
            emission_report (pd.DataFrame): the emission report to process

        Returns:
            pd.DataFrame: the cleaned up report
        """
                
        df.drop(["Verifier Address", 
                 "d.1", 
                 "additional information to facilitate the understanding of the reported average operational energy efficiency indicators"], 
                axis=1, inplace=True)
        
        def get_monitoring_methods(row):
            methods = []
            if row['A'] == 'Yes':
                methods.append('A')
            if row['B'] == 'Yes':
                methods.append('B')
            if row['C'] == 'Yes':
                methods.append('C')
            if row['D'] == 'Yes':
                methods.append('D')
            if row['D.1'] == 'Yes':
                methods.append('D.1')
            return ', '.join(methods) if methods else ''

        df['monitoring_methods'] = df.apply(get_monitoring_methods, axis=1)
        df.drop(['A', 'B', 'C', 'D', 'D.1'], axis=1, inplace=True)
        
        df = df.replace(to_replace="Division by zero!", value=np.nan).infer_objects(copy=False)
        
        df['DoC issue date'] = df['DoC issue date'].replace('DoC not issued', np.nan).infer_objects(copy=False)
        df['DoC expiry date'] = df['DoC expiry date'].replace('DoC not issued', np.nan).infer_objects(copy=False)
        df['DoC issue date'] = pd.to_datetime(df['DoC issue date'], format='%d/%m/%Y')
        df['DoC expiry date'] = pd.to_datetime(df['DoC expiry date'], format='%d/%m/%Y')
        
        df = df.select_dtypes(include=['object']).fillna('Missing')
        
        pattern = r'(\w+)\s\(([\d.]+)\s(.*)\)'
        df[['technical_efficiency_type', 'technical_efficiency_value', 'technical_efficiency_unit']] = df['Technical efficiency'].str.extract(pattern)
        
        column_name_mapping = {
            'IMO Number': 'imo_number',
            'Name': 'name',
            'Ship type': 'ship_type',
            'Reporting Period': 'reporting_period',
            'Technical efficiency': 'technical_efficiency',
            'Port of Registry': 'port_of_registry',
            'Home Port': 'home_port',
            'Ice Class': 'ice_class',
            'DoC issue date': 'doc_issue_date',
            'DoC expiry date': 'doc_expiry_date',
            'Verifier Number': 'verifier_number',
            'Verifier Name': 'verifier_name',
            'Verifier NAB': 'verifier_nab',
            'Verifier Address': 'verifier_address',
            'Verifier City': 'verifier_city',
            'Verifier Accreditation number': 'verifier_accreditation_number',
            'Verifier Country': 'verifier_country',
            'A': 'a',
            'B': 'b',
            'C': 'c',
            'D': 'd',
            'Total fuel consumption [m tonnes]': 'total_fuel_consumption_[m_tonnes]',
            'Fuel consumptions assigned to On laden [m tonnes]': 'fuel_consumptions_assigned_to_on_laden_[m_tonnes]',
            'Total CO₂ emissions [m tonnes]': 'total_co₂_emissions_[m_tonnes]',
            'CO₂ emissions from all voyages between ports under a MS jurisdiction [m tonnes]': 'co₂_emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction_[m_tonnes]',
            'CO₂ emissions from all voyages which departed from ports under a MS jurisdiction [m tonnes]': 'co₂_emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction_[m_tonnes]',
            'CO₂ emissions from all voyages to ports under a MS jurisdiction [m tonnes]': 'co₂_emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction_[m_tonnes]',
            'CO₂ emissions which occurred within ports under a MS jurisdiction at berth [m tonnes]': 'co₂_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth_[m_tonnes]',
            'CO₂ emissions assigned to Passenger transport [m tonnes]': 'co₂_emissions_assigned_to_passenger_transport_[m_tonnes]',
            'CO₂ emissions assigned to Freight transport [m tonnes]': 'co₂_emissions_assigned_to_freight_transport_[m_tonnes]',
            'CO₂ emissions assigned to On laden [m tonnes]': 'co₂_emissions_assigned_to_on_laden_[m_tonnes]',
            'Annual Time spent at sea [hours]': 'annual_time_spent_at_sea_[hours]',
            'Annual average Fuel consumption per distance [kg / n mile]': 'annual_average_fuel_consumption_per_distance_[kg_/_n_mile]',
            'Annual average Fuel consumption per transport work (mass) [g / m tonnes · n miles]': 'annual_average_fuel_consumption_per_transport_work_(mass)_[g_/_m_tonnes_·_n_miles]',
            'Annual average Fuel consumption per transport work (volume) [g / m³ · n miles]': 'annual_average_fuel_consumption_per_transport_work_(volume)_[g_/_m³_·_n_miles]',
            'Annual average Fuel consumption per transport work (dwt) [g / dwt carried · n miles]': 'annual_average_fuel_consumption_per_transport_work_(dwt)_[g_/_dwt_carried_·_n_miles]',
            'Annual average Fuel consumption per transport work (pax) [g / pax · n miles]': 'annual_average_fuel_consumption_per_transport_work_(pax)_[g_/_pax_·_n_miles]',
            'Annual average Fuel consumption per transport work (freight) [g / m tonnes · n miles]': 'annual_average_fuel_consumption_per_transport_work_(freight)_[g_/_m_tonnes_·_n_miles]',
            'Annual average CO₂ emissions per distance [kg CO₂ / n mile]': 'annual_average_co₂_emissions_per_distance_[kg_co₂_/_n_mile]',
            'Annual average CO₂ emissions per transport work (mass) [g CO₂ / m tonnes · n miles]': 'annual_average_co₂_emissions_per_transport_work_(mass)_[g_co₂_/_m_tonnes_·_n_miles]',
            'Annual average CO₂ emissions per transport work (volume) [g CO₂ / m³ · n miles]': 'annual_average_co₂_emissions_per_transport_work_(volume)_[g_co₂_/_m³_·_n_miles]',
            'Annual average CO₂ emissions per transport work (dwt) [g CO₂ / dwt carried · n miles]': 'annual_average_co₂_emissions_per_transport_work_(dwt)_[g_co₂_/_dwt_carried_·_n_miles]',
            'Annual average CO₂ emissions per transport work (pax) [g CO₂ / pax · n miles]': 'annual_average_co₂_emissions_per_transport_work_(pax)_[g_co₂_/_pax_·_n_miles]',
            'Annual average CO₂ emissions per transport work (freight) [g CO₂ / m tonnes · n miles]': 'annual_average_co₂_emissions_per_transport_work_(freight)_[g_co₂_/_m_tonnes_·_n_miles]',
            'Through ice [n miles]': 'through_ice_[n_miles]',
            'Time spent at sea [hours]': 'time_spent_at_sea_[hours]',
            'Total time spent at sea through ice [hours]': 'total_time_spent_at_sea_through_ice_[hours]',
            'Fuel consumption per distance on laden voyages [kg / n mile]': 'fuel_consumption_per_distance_on_laden_voyages_[kg_/_n_mile]',
            'Fuel consumption per transport work (mass) on laden voyages [g / m tonnes · n miles]': 'fuel_consumption_per_transport_work_(mass)_on_laden_voyages_[g_/_m_tonnes_·_n_miles]',
            'Fuel consumption per transport work (volume) on laden voyages [g / m³ · n miles]': 'fuel_consumption_per_transport_work_(volume)_on_laden_voyages_[g_/_m³_·_n_miles]',
            'Fuel consumption per transport work (dwt) on laden voyages [g / dwt carried · n miles]': 'fuel_consumption_per_transport_work_(dwt)_on_laden_voyages_[g_/_dwt_carried_·_n_miles]',
            'Fuel consumption per transport work (pax) on laden voyages [g / pax · n miles]': 'fuel_consumption_per_transport_work_(pax)_on_laden_voyages_[g_/_pax_·_n_miles]',
            'Fuel consumption per transport work (freight) on laden voyages [g / m tonnes · n miles]': 'fuel_consumption_per_transport_work_(freight)_on_laden_voyages_[g_/_m_tonnes_·_n_miles]',
            'CO₂ emissions per distance on laden voyages [kg CO₂ / n mile]': 'co₂_emissions_per_distance_on_laden_voyages_[kg_co₂_/_n_mile]',
            'CO₂ emissions per transport work (mass) on laden voyages [g CO₂ / m tonnes · n miles]': 'co₂_emissions_per_transport_work_(mass)_on_laden_voyages_[g_co₂_/_m_tonnes_·_n_miles]',
            'CO₂ emissions per transport work (volume) on laden voyages [g CO₂ / m³ · n miles]': 'co₂_emissions_per_transport_work_(volume)_on_laden_voyages_[g_co₂_/_m³_·_n_miles]',
            'CO₂ emissions per transport work (dwt) on laden voyages [g CO₂ / dwt carried · n miles]': 'co₂_emissions_per_transport_work_(dwt)_on_laden_voyages_[g_co₂_/_dwt_carried_·_n_miles]',
            'CO₂ emissions per transport work (pax) on laden voyages [g CO₂ / pax · n miles]': 'co₂_emissions_per_transport_work_(pax)_on_laden_voyages_[g_co₂_/_pax_·_n_miles]',
            'CO₂ emissions per transport work (freight) on laden voyages [g CO₂ / m tonnes · n miles]': 'co₂_emissions_per_transport_work_(freight)_on_laden_voyages_[g_co₂_/_m_tonnes_·_n_miles]',
            'Average density of the cargo transported [m tonnes / m³]': 'average_density_of_the_cargo_transported_[m_tonnes_/_m³]'
        }
        df = df.rename(columns=column_name_mapping)
                
        return df

    def load(self, cleaned_emission_report: pd.DataFrame):
        """Loads the new file in the silver location of the bucket

        Args:
            cleaned_emission_report (pd.DataFrame): the cleaned up report
        """
        # upload the file in the silver location
        # update the 'tracker' of the processed files to keep track (update the metadata)
        pass
    
    def run(self):
        etl = ETLPipeline()
        raw_data_list = etl.extract()
        
        for df in raw_data_list:
            etl.tranform(emission_report=df)
            etl.load()

def main():
    pass

if __name__=='__main__':
    main()