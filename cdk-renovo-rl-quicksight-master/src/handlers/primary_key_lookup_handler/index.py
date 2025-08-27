import boto3
import os
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    # Redshift Serverless connection parameters from environment variables
    workgroup_name = os.getenv('REDSHIFT_WORKGROUP_NAME')
    redshift_database = os.getenv('REDSHIFT_DATABASE')
    secret_name = os.getenv('REDSHIFT_SECRET_NAME')
    rl_database = event.get('database').replace('dev-', '').replace('prod-', '')
    table = event.get('table').replace('full_load_', '')
    
    # Verify inputs are not empty
    if not rl_database or not table:
        logger.error(f"Invalid input: {event}")
        raise Exception(f"Invalid input: {event}")
    
    logger.info(f"Event: {event}")

    # Look up the secret ARN by name
    secrets_client = boto3.client('secretsmanager')
    try:
        secret_response = secrets_client.describe_secret(SecretId=secret_name)
        secret_arn = secret_response['ARN']
        logger.info(f"  Retrieved Secret ARN: {secret_arn}")
    except Exception as e:
        logger.error(f"Error occurred while retrieving the secret ARN: {e}")
        raise Exception(f"Error occurred while retrieving the secret ARN: {e}")

    redshift_data_client = boto3.client('redshift-data')

    try:
        dbtype = 'cl' if rl_database.startswith('cl') else rl_database
        pk_query = f"""
            SELECT primarykeys
            FROM metadata_spectrum.pks
            WHERE dbtype = '{dbtype}' AND tablename = '{table}'
        """
        logger.info(f"Retrieving primary keys from metadata_spectrum.pks")

        response = redshift_data_client.execute_statement(
            Database=redshift_database,
            SecretArn=secret_arn,
            WorkgroupName=workgroup_name,
            Sql=pk_query
        )

        # Wait for the primary key query to complete
        query_id = response['Id']
        status = redshift_data_client.describe_statement(Id=query_id)['Status']
        while status not in ["FINISHED", "FAILED", "ABORTED"]:
            time.sleep(1)
            status = redshift_data_client.describe_statement(Id=query_id)['Status']

        if status != "FINISHED":
            failure_details = redshift_data_client.describe_statement(Id=query_id)
            logger.error(f"Error occurred while retrieving primary keys: {failure_details.get('Error')}")
            raise Exception(f"Error occurred while retrieving primary keys: {failure_details.get('Error')}")

        # Retrieve the result of the primary key query
        result = redshift_data_client.get_statement_result(Id=query_id)
        if not result['Records']:
            logger.error(f"No primary keys found for '{table}' table in metadata_spectrum.pks.")
            raise Exception(f"No primary keys found for '{table}' table in metadata_spectrum.pks.")

        primary_keys = result['Records'][0][0]['stringValue']
        
        primary_keys_list = [key.strip() for key in primary_keys.split('|')]
        logger.info(f"Step 2: Retrieved primary keys: {primary_keys}")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise Exception(f"Error occurred: {e}")

    return {
        'statusCode': 200,
        'body': 'PKs retrieved',
        'primary_keys': primary_keys_list,
    }
