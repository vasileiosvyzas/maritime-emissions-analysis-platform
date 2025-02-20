import json
import boto3
import time
import os

from datetime import datetime


# Initialize the Athena client
athena_client = boto3.client("athena")

# Athena configuration
DATABASE = os.environ["DATABASE"]
TABLE = os.environ["TABLE"]
OUTPUT_LOCATION = os.environ["OUTPUT_LOCATION"]

import random, string


def random_string(length):
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length)
    )


def lambda_handler(event, context):
    print(event)

    # Query for ship types
    ship_types_query = f"""
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
    
    SELECT DISTINCT ship_type
    FROM clean_emissions
    ORDER BY ship_type
    """

    # Query for other metadata
    metadata_query = f"""
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
    
    SELECT 
        COUNT(*) as total_ships,
        MIN(reporting_period) as earliest_period,
        MAX(reporting_period) as latest_period
    FROM latest_data
    """

    ship_types = execute_athena_query(ship_types_query)
    metadata = execute_athena_query(metadata_query)[0]

    current_datetime = datetime.now()

    response_body = {
        "results": [
            {
                "ship_types": [row["ship_type"] for row in ship_types],
                "total_ships": metadata["total_ships"],
                "earliest_period": metadata["earliest_period"],
                "latest_period": metadata["latest_period"],
            }
        ],
        "timestamp": current_datetime.strftime("%d-%m-%Y %H:%M:%S"),
        "request_id": random_string(10),
    }

    return {
        "statusCode": 200,
        "body": json.dumps(response_body),
        "headers": {"Content-Type": "application/json"},
    }


def execute_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )

    query_execution_id = response["QueryExecutionId"]

    # Wait for the query to complete (simplified for brevity)
    while True:
        query_status = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = query_status["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break

    if status == "SUCCEEDED":
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        columns = [
            col["Label"]
            for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        ]
        rows = results["ResultSet"]["Rows"][1:]  # Skip the header row
        return [
            dict(zip(columns, [field.get("VarCharValue", "") for field in row["Data"]]))
            for row in rows
        ]
    else:
        raise Exception(f"Query failed with status: {status}")
