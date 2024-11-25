import json
import boto3
import time
import os
import math
from datetime import datetime

# Initialize the Athena client
athena_client = boto3.client('athena')

# Athena configuration
DATABASE = os.environ['DATABASE']
TABLE = os.environ['TABLE']
OUTPUT_LOCATION = os.environ['OUTPUT_LOCATION']

import random, string

def random_string(length):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def execute_athena_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
    )
    
    query_execution_id = response['QueryExecutionId']
    
    # Wait for the query to complete (simplified for brevity)
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
    
    if status == 'SUCCEEDED':
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = results['ResultSet']['Rows'][1:]  # Skip the header row
        return [dict(zip(columns, [field.get('VarCharValue', '') for field in row['Data']])) for row in rows]
    else:
        raise Exception(f"Query failed with status: {status}")

def get_total_results(ship_type):
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

        SELECT COUNT(*) AS total_results
        FROM latest_data
        WHERE ship_type='{ship_type}';
    """
    
    total_results_value = execute_athena_query(query=query)
    return total_results_value


def get_ship_info(ship_type, page, limit):
    
    offset = (page - 1) * limit

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


        SELECT imo_number, name, ship_type, reporting_period, port_of_registry, home_port, ice_class, doc_issue_date, doc_expiry_date, verifier_number, technical_efficiency_value
        FROM latest_data
        WHERE ship_type='{ship_type}'
        ORDER BY imo_number
        OFFSET {offset}
        LIMIT {limit};
    """
    
    ship_info = execute_athena_query(query=query)
    return ship_info

def lambda_handler(event, context):
    print(event)
    ship_type = event['queryStringParameters']['ship_type']
    page = int(event['queryStringParameters']['page'])
    limit = event['queryStringParameters']['limit']
    
    total_results = int(get_total_results(ship_type=ship_type)[0]['total_results'])
    ship_info = get_ship_info(ship_type=ship_type, page=page, limit=limit)
    total_pages = math.ceil(total_results / page)
    
    if page < total_pages:
        next_page_url = f"https://lwjkzp8fki.execute-api.us-east-1.amazonaws.com/dev/ships?ship_type=Oil tanker&page={page + 1}&limit=10"
    else:
        next_page_url = 'null'
        
    if page > 1:
        prev_page_url = f"https://lwjkzp8fki.execute-api.us-east-1.amazonaws.com/dev/ships?ship_type=Oil tanker&page={page - 1}&limit=10"
    else:
        prev_page_url = 'null'

    current_datetime = datetime.now()

    response = {
            'metadata': {
                'timestamp': current_datetime.strftime("%d-%m-%Y %H:%M:%S"),
                'request_id': random_string(10),
                "total_results": total_results,
                "page": page,
                "per_page": limit,
                "total_pages": total_pages,
                "next_page_url": next_page_url,
                "prev_page_url": prev_page_url
            },
            'results': ship_info
    }
    
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json'},
        'body': json.dumps(response)
    }