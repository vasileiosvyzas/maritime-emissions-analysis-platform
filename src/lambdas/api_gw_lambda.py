import json
import boto3
import time
import os

# Initialize the Athena client
athena_client = boto3.client('athena')

# Athena configuration
DATABASE = os.environ['DATABASE']
TABLE = os.environ['TABLE']
OUTPUT_LOCATION = os.environ['OUTPUT_LOCATION']


def lambda_handler(event, context):
    # Extract ship_id from the path parameters
    print(event)
    ship_id = event['pathParameters']['ship_id']
    
    # Define the query
    query = f"SELECT * FROM {TABLE} WHERE imo_number = {ship_id};"
    
    # Start the query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_LOCATION}
    )
    
    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']
    
    # Wait for the query to complete
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        print('After query execution')
        print(response)
        status = query_status['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        
        time.sleep(1)
    
    print(status)
    # If the query failed, return an error
    if status != 'SUCCEEDED':
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Athena query failed'})
        }
    
    # Get the query results
    result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    
    # Parse the results
    columns = result['ResultSet']['ResultSetMetadata']['ColumnInfo']
    rows = result['ResultSet']['Rows']
    print('ROWS')
    print(rows)
    result_data = []
    
    for row in rows[1:]:
        row_dict = {}
        for i in range(len(columns)):
            print(row['Data'][i])
            if row['Data'][i]:
                row_dict[columns[i]['Name']] = row['Data'][i]['VarCharValue']
            else:
                row_dict[columns[i]['Name']] = 'NaN'
            result_data.append(row_dict)
    
    return {
        'statusCode': 200,
        'body': json.dumps(result_data)
    }
