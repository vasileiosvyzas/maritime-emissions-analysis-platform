import os
import pandas as pd

from dotenv import load_dotenv
from google.cloud import storage
from io import StringIO, BytesIO

load_dotenv()

class GoogleCloudStorageManager():
    """Contains functions and data to manage the storage and management of files in the buckets."""
    
    def __init__(self):
        self.client = storage.Client(project=os.environ['GCP_PROJECT_ID'])
        self.bucket = self.client.bucket(bucket_name=os.environ['BUCKET_NAME'])
        
    def download_file_into_memory(self, blob_name:str, bucket_layer:str) -> pd.DataFrame:
        """Downloads a file into memory and creates a pandas dataframe

        Args:
            blob_name (str): the name of the file
            bucket_layer (str): it can be one of three options (bronze, silver, gold)

        Returns:
            pd.DataFrame: a dataframe with the contents of the file
        """
        
        try:
            cloud_file = f"{bucket_layer}/{blob_name}"
            blob = self.bucket.blob(blob_name=cloud_file)
            contents = blob.download_as_bytes()
            df = pd.read_csv(StringIO(contents.decode('utf-8')))
            
            return df
        except Exception as e:
            print(f"Failed to fetch reports_metadata.csv from GCS: {e.with_traceback}")
            
    def upload_file(self, source_file:str, bucket_layer:str, destination_blob_name:str):
        """Uploads the source file to a specific location in the bucket

        Args:
            bucket_layer (str): one of three options (bronze, silver, gold)
            source_file (str): the name of the file to upload
            destination_blob_name (str): the name of the file in the bucket location
        """
        
        try:
            blob = self.bucket.blob(f"{bucket_layer}/{destination_blob_name}")
            blob.upload_from_filename(source_file)
            
            print(f"Uploaded {source_file} to gs://{self.bucket}/{destination_blob_name}")
        except Exception as e:
            print(e)
            
    def upload_dataframe_from_memory(self, bucket_layer:str, dataframe:pd.DataFrame, destination_blob_name:str):
        """Uploads the contents of a file that are in memory to a
        file in the bucket location specified.

        Args:
            bucket_layer (str): one of three options (bronze, silver, gold)
            destination_blob_name (str): the name of the file in the bucket
        """
        try:
            blob = self.bucket.blob(f"{bucket_layer}/{destination_blob_name}")
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False)
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        except Exception as e:
            print(e)
        