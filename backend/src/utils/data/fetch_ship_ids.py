import os
import boto3
import logging

import awswrangler as wr
import pandas as pd

from dotenv import load_dotenv
from typing import Optional
from botocore.exceptions import ClientError

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_aws_session() -> boto3.Session:
    """Creates the session to communicate with Athena.

    Returns:
        boto3.Session: The configured session if there are no problems.
    """

    try:
        session = boto3.session.Session(
            region_name=os.environ["REGION"],
            aws_access_key_id=os.environ["ACCESS_KEY"],
            aws_secret_access_key=os.environ["SECRET_KEY"],
        )

        return session
    except ClientError as e:
        logger.error(f"Failed to create the AWS session: {e}")
        raise


def fetch_ship_ids() -> Optional[pd.DataFrame]:
    """Fetch distinct ship IMO numbers and names from the latest version of each year.

    Returns:
        pd.DataFrame: DataFrame containing 'imo_number' and 'name' columns
    """

    DATABASE = os.environ["DATABASE"]
    TABLE = os.environ["TABLE"]

    try:
        aws_session = get_aws_session()

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

        unique_ships = wr.athena.read_sql_query(
            query, database=DATABASE, boto3_session=aws_session
        )
        logger.info(
            f"Fetched all the ships from the database. There are {unique_ships.shape[0]} ships in the database."
        )

        return unique_ships

    except Exception as e:
        logger.error(f"An error occured when fetching the ship IDs: {e}")
        raise
