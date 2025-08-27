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
        schema_query = f"""
            SELECT schemaname
            FROM svv_external_tables
            WHERE tablename = 'full_load_{table}'
            AND schemaname like 'cl%'
        """
        logger.info("Step 2: Retrieving primary keys from unifieddb_spectrum.pks")

        response = redshift_data_client.execute_statement(
            Database=redshift_database,
            SecretArn=secret_arn,
            WorkgroupName=workgroup_name,
            Sql=schema_query
        )

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
            logger.error(f"No shared schemas found for '{table}' table in svv_external_tables.")
            raise Exception(f"No shared schemas found for '{table}' table in svv_external_tables.")
        
        schemas = [row[0]['stringValue'] for row in result['Records']]

        tables_missing_views = []

        valid_schemas = []
        for schema in schemas:
            non_spectrum_schema = schema.replace('_spectrum', '')
            view_check_query = f"""
                SELECT COUNT(*)
                FROM pg_views
                WHERE schemaname = '{non_spectrum_schema}'
                AND viewname = 'v_{table}_spectrum'
            """
            logger.info(f"Checking if view exists in schema {non_spectrum_schema}")

            response = redshift_data_client.execute_statement(
                Database=redshift_database,
                SecretArn=secret_arn,
                WorkgroupName=workgroup_name,
                Sql=view_check_query
            )

            # Wait for the view check query to complete
            query_id = response['Id']
            status = redshift_data_client.describe_statement(Id=query_id)['Status']
            while status not in ["FINISHED", "FAILED", "ABORTED"]:
                time.sleep(1)
                status = redshift_data_client.describe_statement(Id=query_id)['Status']

            if status != "FINISHED":
                failure_details = redshift_data_client.describe_statement(Id=query_id)
                logger.error(f"Error occurred while checking for view: {failure_details.get('Error')}")
                raise Exception(f"Error occurred while checking for view: {failure_details.get('Error')}")

            # Retrieve the result of the view check query
            result = redshift_data_client.get_statement_result(Id=query_id)
            view_count = int(result['Records'][0][0]['longValue'])

            if view_count == 0:
                tables_missing_views.append({
                    'table': table,
                    'schema': non_spectrum_schema
                })
                logger.warning(f"View v_{table}_spectrum not found in schema {non_spectrum_schema}.")
            else:
                valid_schemas.append(non_spectrum_schema)

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise Exception(f"Error occurred: {e}")

    return {
        'statusCode': 200,
        'body': 'Related schemas retrieved',
        'schemas': valid_schemas,
        'tables_missing_views': tables_missing_views
    }
