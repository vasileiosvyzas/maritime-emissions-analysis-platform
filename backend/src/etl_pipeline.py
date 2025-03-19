import pandas as pd
import numpy as np

from google_cloud_storage_manager import GoogleCloudStorageManager

class ETLPipeline():
    def __init__(self):
        self.storage_client = GoogleCloudStorageManager()

    def extract(self, file_name: str) -> pd.DataFrame:
        """Downloads the new CO2 emission report from the bronze location in the bucket.
        It needs to identify which is the new file that has been added comparing with the "old" ones 
        that have already been processed.

        Args:
            bucket_name (str): the name of the bucket

        Returns:
            pd.DataFrame: the new CO2 emission report in pandas dataframe
        """
        # list all files
        # identify the new files that haven't been processed (list of files?)
        # Download a file as a stream???
        
        pass

    def tranform(self, emission_report: pd.DataFrame) -> pd.DataFrame:
        """Does all the transformations on the emission report to make it clean

        Args:
            emission_report (pd.DataFrame): the emission report to process

        Returns:
            pd.DataFrame: the cleaned up report
        """
        # remove useless columns (i.e. addressses)
        # concatenate columns
        # denormalize dataset??
        pass

    def load(self, cleaned_emission_report: pd.DataFrame):
        """Loads the new file in the silver location of the bucket

        Args:
            cleaned_emission_report (pd.DataFrame): the cleaned up report
        """
        # upload the file in the silver location
        # update the 'tracker' of the processed files to keep track
        pass