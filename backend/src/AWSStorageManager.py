import os
import boto3
import pandas as pd

from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

class AWSStorageManager():
    def __init__(self):
        self.client = boto3.client(
            's3', 
            aws_access_key_id=os.environ['AWS_ACCESS_KEY'], 
            aws_secret_access_key=os.environ['AWS_SECRET_KEY'], 
            region_name=os.environ['REGION_NAME']
        )
    
    def download_file_from_bucket(self, bucket_layer:str, blob_name: str):
        self.client.download_file(
            os.environ["BUCKET_NAME"], f"{bucket_layer}/{blob_name}", "reports_metadata.csv"
        )
        reports_df_old = pd.read_csv("reports_metadata.csv")
        return reports_df_old
    
    def upload_file(self, file_name, bucket, object_name=None):
        """
        Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """


        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        s3_client = boto3.client(
            "s3", 
            aws_access_key_id=os.environ['AWS_ACCESS_KEY'], 
            aws_secret_access_key=os.environ['AWS_SECRET_KEY'], 
            region_name=os.environ['REGION_NAME']
        )

        try:
            # logger.info(
            #     f"Uploading the file: {file_name} to the bucket: {bucket} with object name: {object_name}"
            # )

            s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            # logger.error(e)
            return False
        return True
    
    
    def upload_csv_to_s3(self):
        reports_df_updated.to_csv("reports_metadata.csv", index=False)

        csv_buffer = StringIO()
        reports_df_updated.to_csv(csv_buffer, index=False)

        # Upload the CSV to S3
        s3.put_object(
            Bucket=os.environ["BUCKET_NAME"],
            Key=f"raw/reports_metadata.csv",
            Body=csv_buffer.getvalue(),
        )