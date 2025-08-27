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
    table = event.get('table')
    schemas = event.get('schemas')
    
    # Verify inputs are not empty
    if not rl_database or not table or not schemas:
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
        if len(schemas) > 1:
            union_queries = []
            for schema in schemas:
                union_queries.append(f"SELECT * FROM {schema}.v_{table}_spectrum")
            unified_view_query = f"""
                CREATE OR REPLACE VIEW unifieddb.v_{table}_spectrum AS
                {' UNION ALL '.join(union_queries)}
                WITH NO SCHEMA BINDING
            """
            logger.info(f"Step 3: Creating or updating unified view for {table} in unifieddb schema")
            
            logger.info(f"Unified view query: {unified_view_query}")

            response = redshift_data_client.execute_statement(
                Database=redshift_database,
                SecretArn=secret_arn,
                WorkgroupName=workgroup_name,
                Sql=unified_view_query
            )

            # Wait for the unified view creation to complete
            query_id = response['Id']
            status = redshift_data_client.describe_statement(Id=query_id)['Status']
            while status not in ["FINISHED", "FAILED", "ABORTED"]:
                time.sleep(1)
                status = redshift_data_client.describe_statement(Id=query_id)['Status']

            if status == "FINISHED":
                logger.info("Unified view created or replaced successfully.")
            else:
                failure_details = redshift_data_client.describe_statement(Id=query_id)
                logger.error(f"Error occurred while creating unified view: {failure_details.get('Error')}")
                raise Exception(f"Error occurred while creating unified view: {failure_details.get('Error')}")
        elif len(schemas) == 1:
            logger.warning(f"Not enough schemas available to create unified view. Creating a copy of the only view instead: {schemas[0]}.v_{table}_spectrum")
            copy_view_query = f"""
                CREATE OR REPLACE VIEW unifieddb.v_{table}_spectrum AS
                SELECT * FROM {schemas[0]}.v_{table}_spectrum
                WITH NO SCHEMA BINDING
            """
            
            logger.info(f"Copy view query: {copy_view_query}")
            
            response = redshift_data_client.execute_statement(
                Database=redshift_database,
                SecretArn=secret_arn,
                WorkgroupName=workgroup_name,
                Sql=copy_view_query
            )
            
            # Wait for the copy view creation to complete
            query_id = response['Id']
            status = redshift_data_client.describe_statement(Id=query_id)['Status']
            while status not in ["FINISHED", "FAILED", "ABORTED"]:
                time.sleep(1)
                status = redshift_data_client.describe_statement(Id=query_id)['Status']
            
            if status == "FINISHED":
                logger.info("Copy view created or replaced successfully.")
            else:
                failure_details = redshift_data_client.describe_statement(Id=query_id)
                logger.error(f"Error occurred while creating copy view: {failure_details.get('Error')}")
                raise Exception(f"Error occurred while creating copy view: {failure_details.get('Error')}")
        else:
            logger.error(f"No schemas available to create unified view.")
            raise Exception(f"No schemas available to create unified view.")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise Exception(f"Error occurred: {e}")

    return {
        'statusCode': 200,
        'body': 'Unified view created or replaced successfully.'
    }
