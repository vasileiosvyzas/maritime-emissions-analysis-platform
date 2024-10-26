import json
import boto3
import time
import os
import math
import random, string
from datetime import datetime
from typing import Dict, Optional, TypedDict
from dataclasses import dataclass
from decimal import Decimal

# Initialize the Athena client
athena_client = boto3.client("athena")

DATABASE = os.environ["DATABASE"]
TABLE = os.environ["TABLE"]
OUTPUT_LOCATION = os.environ["OUTPUT_LOCATION"]
API_URL = os.environ['API_URL']

current_datetime = datetime.now()

def random_string(length):
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length)
    )


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


joined_table = f"""
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
"""

base_query = """
    SELECT imo_number, name, ship_type, reporting_period, total_co2_emissions, 
        co2_emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction, 
        co2_emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction, 
        co2_emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction, 
        co2_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth, 
        co2_emissions_assigned_to_passenger_transport, 
        co2_emissions_assigned_to_freight_transport, co2_emissions_assigned_to_on_laden
    FROM latest_data
"""

def get_total_results():
    statement = f"""
        SELECT COUNT(*) AS total_results
        FROM latest_data
    """
    
    query = joined_table + statement

    total_results_value = execute_athena_query(query=query)
    return total_results_value

def emissions_data_without_conditions(page, limit):
    offset = (page - 1) * limit
    
    pagination_query = f"""
        ORDER BY imo_number, reporting_period DESC
        OFFSET {offset}
        LIMIT {limit}
    """
    
    query = joined_table + base_query + pagination_query
    data_response = execute_athena_query(query=query)
    print(data_response)

    print(get_total_results())
    total_results = int(get_total_results()[0]['total_results'])

    print(total_results)

    total_pages = math.ceil(total_results / page)

    print(total_results)
    print(total_pages)
    
    if page < total_pages:
        next_page_url = f"{API_URL}/emissions?page={page + 1}&limit=10"
    else:
        next_page_url = 'null'
        
    if page > 1:
        prev_page_url = f"{API_URL}/emissions?page={page - 1}&limit=10"
    else:
        prev_page_url = 'null'
    
    return {
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
        'results': data_response
    }
    
def emissions_per_ship_id(ship_id):
    print('in am in func the parameters are ship id')

    condition = f"""
        WHERE imo_number = {ship_id}
        ORDER BY reporting_period DESC;
    """
    query = joined_table + base_query + condition

    response = execute_athena_query(query=query)
    
    statement = f"""
        SELECT COUNT(*) AS total_results
        FROM latest_data
        WHERE imo_number = {ship_id};
    """
    
    query = joined_table + statement

    total_results_value = execute_athena_query(query=query)
    total_results = int(total_results_value[0]['total_results'])
    
    print(total_results)

    return {
        'metadata': {
            'timestamp': current_datetime.strftime("%d-%m-%Y %H:%M:%S"),
            'request_id': random_string(10),
            "total_results": total_results,
        },
        'results': response
    }

def emissions_per_ship_type_and_year(ship_type, year, page, limit):
    print('Our parameters are ship_type and year')
    offset = (page - 1) * limit
    
    condition_query = f"""
        WHERE ship_type = '{ship_type}' AND reporting_period={year}
        ORDER BY imo_number, reporting_period DESC
        OFFSET {offset}
        LIMIT {limit};
    """
    
    print(condition_query)

    query = joined_table + base_query + condition_query
    data_response = execute_athena_query(query=query)
    print(data_response)

    statement = f"""
        SELECT COUNT(*) AS total_results
        FROM latest_data
        WHERE ship_type = '{ship_type}' AND reporting_period={year};
    """
    
    query = joined_table + statement

    total_results_value = execute_athena_query(query=query)
    total_results = int(total_results_value[0]['total_results'])

    total_pages = math.ceil(total_results / page)

    print(total_results)
    print(total_pages)
    
    if page < total_pages:
        next_page_url = f"{API_URL}/emissions?ship_type={ship_type}&year={year}&page={page + 1}&limit=10"
    else:
        next_page_url = 'null'
        
    if page > 1:
        prev_page_url = f"{API_URL}/emissions?ship_type={ship_type}&year={year}&page={page - 1}&limit=10"
    else:
        prev_page_url = 'null'
    
    return {
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
        'results': data_response
    }

def emissions_per_ship_type(ship_type, page, limit):
    offset = (page - 1) * limit
    
    condition_query = f"""
        WHERE ship_type = '{ship_type}'
        ORDER BY imo_number, reporting_period DESC
        OFFSET {offset}
        LIMIT {limit};
    """
    
    query = joined_table + base_query + condition_query
    data_response = execute_athena_query(query=query)

    statement = f"""
        SELECT COUNT(*) AS total_results
        FROM latest_data
        WHERE ship_type = '{ship_type}';
    """
    
    query = joined_table + statement

    total_results_value = execute_athena_query(query=query)
    total_results = int(total_results_value[0]['total_results'])
    total_pages = math.ceil(total_results / page)
    
    if page < total_pages:
        next_page_url = f"{API_URL}/emissions?ship_type={ship_type}&page={page + 1}&limit=10"
    else:
        next_page_url = 'null'
        
    if page > 1:
        prev_page_url = f"{API_URL}/emissions?ship_type={ship_type}&page={page - 1}&limit=10"
    else:
        prev_page_url = 'null'
    
    return {
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
        'results': data_response
    }
    
def emissions_per_year(year, page, limit):
    offset = (page - 1) * limit
    
    condition_query = f"""
        WHERE reporting_period = {year}
        ORDER BY imo_number, reporting_period DESC
        OFFSET {offset}
        LIMIT {limit};
    """
    query = joined_table + base_query + condition_query
    data_response = execute_athena_query(query=query)

    statement = f"""
        SELECT COUNT(*) AS total_results
        FROM latest_data
        WHERE reporting_period = {year};
    """
    
    query = joined_table + statement

    total_results_value = execute_athena_query(query=query)
    total_results = int(total_results_value[0]['total_results'])
    total_pages = math.ceil(total_results / page)
    
    if page < total_pages:
        next_page_url = f"{API_URL}/emissions?year={year}&page={page + 1}&limit=10"
    else:
        next_page_url = 'null'
        
    if page > 1:
        prev_page_url = f"{API_URL}/emissions?year={year}&page={page - 1}&limit=10"
    else:
        prev_page_url = 'null'
    
    return {
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
        'results': data_response
    }

@dataclass
class QueryParams:
    limit: int = 10  # Default limit
    page: int = 1    # Default page
    ship_id: Optional[str] = None
    ship_type: Optional[str] = None
    year: Optional[int] = None

def parse_query_parameters(event: Dict) -> QueryParams:
    """
    Parse and validate query parameters from API Gateway event.
    Returns a QueryParams object with all parameters (using defaults where not provided).
    """
    # Get query parameters from event, defaulting to empty dict if None
    query_params = event.get('queryStringParameters', {}) or {}
    
    # Parse and validate parameters with type conversion
    try:
        params = QueryParams(
            limit=int(query_params.get('limit', 10)),
            page=int(query_params.get('page', 1)),
            ship_id=query_params.get('ship_id'),
            ship_type=query_params.get('ship_type'),
            year=int(query_params.get('year')) if query_params.get('year') else None
        )
        
        # Validate limit and page
        if params.limit < 1 or params.limit > 100:
            raise ValueError("Limit must be between 1 and 100")
        if params.page < 1:
            raise ValueError("Page must be greater than 0")
            
        return params
        
    except ValueError as e:
        raise ValueError(f"Invalid parameter value: {str(e)}")

def determine_query_type(params: QueryParams) -> str:
    """
    Determine which type of query to execute based on provided parameters.
    Returns a string identifier for the query type.
    """
    if params.ship_id:
        return emissions_per_ship_id(ship_id=params.ship_id)
    elif params.ship_type and params.year:
        return emissions_per_ship_type_and_year(ship_type=params.ship_type, year=params.year, page=params.page, limit=params.limit)
    elif params.ship_type:
        return emissions_per_ship_type(ship_type=params.ship_type, page=params.page, limit=params.limit)
    elif params.year:
        return emissions_per_year(year=params.year, page=params.page, limit=params.limit)
    else:
        return emissions_data_without_conditions(page=params.page, limit=params.limit)

# Example usage in Lambda handler
def lambda_handler(event, context):
    print(event)
    try:
        params = parse_query_parameters(event)
        print(params)

        print(type(params.year))
        print(params.year)
        query_response = determine_query_type(params)
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(query_response),
        }  
        
    except ValueError as e:
        return {
            "statusCode": 400,
            "body": {"error": str(e)}
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": {"error": "Internal server error"}
        }