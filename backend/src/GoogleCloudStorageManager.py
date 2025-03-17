import os
import pandas as pd

from dotenv import load_dotenv
from google.cloud import storage
from io import StringIO, BytesIO

load_dotenv()

class GoogleCloudStorageManager():
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.client = storage.Client(project=os.environ['GCP_PROJECT_ID'])
        
    def download_file_as_stream(self, blob_name:str, bucket_layer: str):        
        cloud_file = f"{bucket_layer}/{blob_name}"
        
        # Use BytesIO to capture the binary stream
        file_obj = BytesIO()

        blob = self.bucket.blob(cloud_file)
        blob.download_to_file(file_obj)
        
        # Seek to start of buffer and decode to text for Pandas
        file_obj.seek(0)
        
        df = pd.read_csv(StringIO(file_obj.read().decode('utf-8')))
        return df
    
    def upload_file_to_gcs(self, local_file_name, destination_blob_name):
        """Upload a file to GCS."""
        
        try:
            blob = self.bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_name)
            print(f"Uploaded {local_file_name} to gs://{self.bucket_name}/{destination_blob_name}")
        except Exception as e:
            print(e)