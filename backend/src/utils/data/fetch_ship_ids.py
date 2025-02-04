import os
import boto3
import awswrangler as wr
from dotenv import load_dotenv
load_dotenv()

def fetch_ship_ids():
    DATABASE = os.environ['DATABASE']
    TABLE = os.environ['TABLE']
    OUTPUT_LOCATION = os.environ['QUERY_LOCATION']
    
    my_session = boto3.session.Session(
        region_name=os.environ['REGION'], 
        aws_access_key_id=os.environ['ACCESS_KEY'], 
        aws_secret_access_key=os.environ['SECRET']
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
        
        SELECT DISTINCT imo_number, name FROM latest_data;
    """
    
    distinc_imo_numbers = wr.athena.read_sql_query(query, database=DATABASE, boto3_session=my_session)
    print(distinc_imo_numbers.shape)
    
    return distinc_imo_numbers