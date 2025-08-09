import logging
import datetime
import re

import pandas as pd
import numpy as np
from typing import List, Optional, Dict
from sys import stdout
from google_cloud_storage_manager import GoogleCloudStorageManager

pd.set_option('future.no_silent_downcasting', True)

logger = logging.getLogger("mylogger")

logger.setLevel(logging.DEBUG)
logFormatter = logging.Formatter(
    "%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
)
consoleHandler = logging.StreamHandler(stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

class ETLPipeline():
    def __init__(self):
        self.storage_client = GoogleCloudStorageManager()

    def extract(self) -> Dict[str, pd.DataFrame]:
        """Downloads the new CO2 emission report from the bronze location in the bucket.
        It needs to identify which is the new file that has been added comparing with the "old" ones 
        that have already been processed.

        Returns:
            pd.DataFrame: the new CO2 emission report in pandas dataframe
        """
        logger.info('Extract function: Downloading the files')
        
        df_to_process = dict()
        blobs = self.storage_client.client.list_blobs(self.storage_client.bucket, prefix='bronze-bucket/')
        logger.info("Blobs:")
        for blob in blobs:
            if blob.name.endswith('.xlsx'):
                logger.info(blob.name)
                if blob.metadata['processed_by_ETL'] == 'False':
                    logger.info("Reading files contents in memory")
                    bucket_layer, year, filename = blob.name.split('/')
                    df = self.storage_client.download_file_into_memory(blob_name=f"{year}/{filename}", bucket_layer=bucket_layer)
                    logger.info("Adding dataset contents to list")
                    df_to_process[blob.name] = df
                    
        logger.info('==> Extraction is done. <==')
        
        return df_to_process
    
    def _clean_column_name(text):
        """Clean column names with special handling for common patterns"""
        if text == 'IMO Number.1':
            text = 'ship_company_imo_number'
        elif text == 'Name.1':
            text = 'ship_company_name'
        
        # Handle special characters
        replacements = {
            'CO₂': 'co2',
            'CH₄': 'ch4',
            'DoC': 'doc',
            'MS': 'ms',
            'NAB': 'nab',
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Remove content within brackets
        text = re.sub(r'\[.*?\]', '', text)
        # Remove asterisks
        text = re.sub(r'\*+', '', text)
        # Replace spaces and special characters with underscores
        text = re.sub(r'[\s\-\.]+', '_', text)
        # Convert to lowercase
        text = text.lower()
        # Clean up underscores
        text = text.strip('_')
        text = re.sub(r'_+', '_', text)
        
        return text        

    def tranform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Does all the transformations on the emission report to make it clean

        Args:
            emission_report (pd.DataFrame): the emission report to process

        Returns:
            pd.DataFrame: the cleaned up report
        """
        
        logger.info('Transform function: Cleaning the dataset')
        if 'Verifier Number' in df.columns:
            df.drop(
                columns=["Verifier Address", 'Verifier Number', 'D.1', 'Additional information to facilitate the understanding of the reported average operational energy efficiency indicators'], 
                axis=1, inplace=True
            )
        else:
            df.drop(columns=["Verifier Address", 'D.1', 'Additional information to facilitate the understanding of the reported average operational energy efficiency indicators'], 
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
            return ', '.join(methods) if methods else ''

        logger.info('Creating the monitoring methods columns')
        df['monitoring_methods'] = df.apply(get_monitoring_methods, axis=1)
        df.drop(['A', 'B', 'C', 'D'], axis=1, inplace=True)
        
        logger.info('Replacing the Division by zero! and DoC not issued values with NaN')
        df = df.replace(to_replace="Division by zero!", value=np.nan).infer_objects(copy=False)
        df['DoC issue date'] = df['DoC issue date'].replace('DoC not issued', np.nan).infer_objects(copy=False)
        df['DoC expiry date'] = df['DoC expiry date'].replace('DoC not issued', np.nan).infer_objects(copy=False)
        df['DoC issue date'] = pd.to_datetime(df['DoC issue date'], format='%d/%m/%Y')
        df['DoC expiry date'] = pd.to_datetime(df['DoC expiry date'], format='%d/%m/%Y')
        
        logger.info("Fill NA at the object columns with the placeholder 'Missing'")
        object_columns = ['Name', 'Ship type', 'Technical efficiency', 'Port of Registry',
                          'Home Port', 'Ice Class', 'Verifier Name',
                          'Verifier NAB', 'Verifier City', 'Verifier Accreditation number',
                          'Verifier Country', 'monitoring_methods']
        df[object_columns] = df[object_columns].fillna('Missing')
        
        logger.info('Creating the technical efficiency columns')
        pattern = r'(\w+)\s\(([\d.]+)\s(.*)\)'
        df[['technical_efficiency_type', 'technical_efficiency_value', 'technical_efficiency_unit']] = df['Technical efficiency'].str.extract(pattern)
        df.drop(['Technical efficiency'], axis=1, inplace=True)
        df['technical_efficiency_value'] = df['technical_efficiency_value'].astype('float')
        
        column_name_mapping = {
            'IMO Number': 'imo_number',
            'Name': 'name',
            'Ship type': 'ship_type',
            'Reporting Period': 'reporting_period',
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
            'Average density of the cargo transported [m tonnes / m³]': 'average_density_of_the_cargo_transported_[m_tonnes_/_m³]',
            'IMO Number.1': 'ship_company_imo_number', 
            'Name.1': 'ship_company_name'
        }
        column_difference = list(set(df.columns).difference(column_name_mapping.keys()))
        column_difference.remove('technical_efficiency_unit')
        column_difference.remove('technical_efficiency_value')
        column_difference.remove('monitoring_methods')
        column_difference.remove('technical_efficiency_type')
        
        logger.info("Renaming the new columns")
        additional_column_name_mapping = {col:self._clean_column_name(col) for col in column_difference}
        
        logger.info("Combining the dictionaries with the column names")
        column_names = column_name_mapping | additional_column_name_mapping
        
        logger.info('Applied the column renaming')
        df = df.rename(columns=column_names)
        
        logger.info('==> Transformation is done. <==')
        
        return df

    def load(self, clean_dataframe: pd.DataFrame, report_name:str, bucket_layer:str):
        """Loads the new file in the silver location of the bucket

        Args:
            cleaned_emission_report (pd.DataFrame): the cleaned up report
        """        
        logger.info(f"uploading the clean file: {report_name} to the silver bucket")        
        self.storage_client.upload_parquet_file_to_bucket(
            bucket_layer=bucket_layer, 
            dataframe=clean_dataframe, 
            destination_blob_name=report_name)
        
        logger.info('Updating the metadata of the files in the silver bucket')
        blob = self.storage_client.bucket.get_blob(f"{bucket_layer}/{report_name}")
        metageneration_match_precondition = None
        metadata = {'processed_by_ETL': True, 'processed_date':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        blob.metadata = metadata
        blob.patch(if_metageneration_match=metageneration_match_precondition)
        
        logger.info('==> Loading is done. <==')
        
    
    def run(self):
        raw_data_list = self.extract()
        
        for df_name, df_contents in raw_data_list.items():
            transformed_df = self.tranform(df=df_contents)
            
            bucket_layer, year, filename =df_name.split('/')
            print()
            self.load(clean_dataframe=transformed_df, report_name=f"{year}/{filename.replace('xlsx', 'parquet')}", bucket_layer='silver-bucket')
            
            logger.info('Updating the metadata of the files in the bronze bucket')
            blob = self.storage_client.bucket.get_blob(f"bronze-bucket/{year}/{filename}")
            metageneration_match_precondition = None
            metadata = {'processed_by_ETL': True, 'processed_date':datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            blob.metadata = metadata
            blob.patch(if_metageneration_match=metageneration_match_precondition)

def main():
    etl = ETLPipeline()
    etl.run()

if __name__=='__main__':
    main()