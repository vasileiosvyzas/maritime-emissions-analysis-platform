import pandas as pd
import numpy as np
from typing import List, Optional

from google_cloud_storage_manager import GoogleCloudStorageManager

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
        # remove useless columns (i.e. addressses)
        # concatenate columns
        
        df.drop(["Verifier Address", 
                 "d.1", 
                 "additional information to facilitate the understanding of the reported average operational energy efficiency indicators"], 
                axis=1, inplace=True)
        
        # Create the monitoring_methods column
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
        
        # rename the columns
        # complete the rest of the ETL
        
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