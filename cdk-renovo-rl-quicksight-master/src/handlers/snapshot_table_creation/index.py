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
    event_table_type = event.get('event_table_type')
    columns = event.get('columns')
    pks = event.get('pks')
    
    # Verify inputs are not empty
    if not rl_database or not table or not event_table_type or not columns or not pks:
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
        cdc_table_exist = False
        
        def cdc_check_query():            
            cdc_check_query = f"""
                SELECT COUNT(*)
                FROM svv_external_tables
                WHERE schemaname = '{rl_database}_spectrum'
                AND tablename = 'cdc_{table}'
            """
            logger.info("Checking if CDC table exists")

            response = redshift_data_client.execute_statement(
                Database=redshift_database,
                SecretArn=secret_arn,
                WorkgroupName=workgroup_name,
                Sql=cdc_check_query
            )

            # Wait for the CDC check query to complete
            query_id = response['Id']
            status = redshift_data_client.describe_statement(Id=query_id)['Status']
            while status not in ["FINISHED", "FAILED", "ABORTED"]:
                time.sleep(1)
                status = redshift_data_client.describe_statement(Id=query_id)['Status']

            if status != "FINISHED":
                failure_details = redshift_data_client.describe_statement(Id=query_id)
                logger.error(f"Error occurred while checking for CDC table: {failure_details.get('Error')}")
                raise Exception(f"Error occurred while checking for CDC table: {failure_details.get('Error')}")

            # Retrieve the result of the CDC check query
            result = redshift_data_client.get_statement_result(Id=query_id)
            cdc_table_count = int(result['Records'][0][0]['longValue'])
            if cdc_table_count > 0:
                return True
            else:
                return False

        if event_table_type == 'full_load':
            if cdc_check_query():
                cdc_table_exist = True
        elif event_table_type == 'cdc':
            cdc_table_exist = True
        else:
            logger.warning(f"Invalid event_table_type: {event_table_type}")
            if cdc_check_query():
                cdc_table_exist = True

        if cdc_table_exist:
            partition_by_clause = ', '.join(pks)
            column_list = ', '.join(columns)
            
            dynamic_query = f"""
            DROP TABLE IF EXISTS {rl_database}.{table}_snapshot;
            CREATE TABLE {rl_database}.{table}_snapshot AS
            WITH full_load_max AS (
                SELECT MAX(ingestion_timestamp) AS max_ts
                FROM {rl_database}_spectrum.full_load_{table}
            ),
            full_load_max_parts AS (
                SELECT
                    DATE_PART(year, max_ts)::int AS yr,
                    DATE_PART(month, max_ts)::int AS mo,
                    DATE_PART(day, max_ts)::int AS dy,
                    max_ts
                FROM full_load_max
            ),
            full_data AS (
                SELECT *
                FROM {rl_database}_spectrum.full_load_{table}
            ),
            cdc_data AS (
                SELECT *
                FROM {rl_database}_spectrum.cdc_{table} AS cdc
                WHERE ingestion_timestamp > (SELECT max_ts FROM full_load_max)
                    AND (
                        (CAST(cdc.partition_0 AS int) = (SELECT yr FROM full_load_max_parts) AND
                        CAST(cdc.partition_1 AS int) = (SELECT mo FROM full_load_max_parts) AND
                        CAST(cdc.partition_2 AS int) >= (SELECT dy FROM full_load_max_parts))
                        OR
                        (CAST(cdc.partition_0 AS int) = (SELECT yr FROM full_load_max_parts) AND
                        CAST(cdc.partition_1 AS int) > (SELECT mo FROM full_load_max_parts))
                        OR
                        (CAST(cdc.partition_0 AS int) > (SELECT yr FROM full_load_max_parts))
                    )
            ),
            combined AS (
                SELECT
                    {column_list}
                FROM full_data
                UNION ALL
                SELECT
                    {column_list}
                FROM cdc_data
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY {partition_by_clause} ORDER BY ingestion_timestamp DESC, ingestion_seq DESC) AS rn
                FROM combined
            )
            SELECT
                '{rl_database}' as db,
                *
            FROM ranked
            WHERE rn = 1 AND op != 'D';
            """
        else:
            # CDC table does not exist, create a simple view of the full load data
            dynamic_query = f"""
            DROP TABLE IF EXISTS {rl_database}.{table}_snapshot;
            CREATE TABLE {rl_database}.{table}_snapshot AS
            SELECT
                '{rl_database}' as db, 
                *,
                1 as rn -- for compatibility with the CDC view
            FROM {rl_database}_spectrum.full_load_{table};
            """
        
        logger.info(f"Dynamic Query: {dynamic_query}")

        # Execute the dynamic query to create or replace the view
        response = redshift_data_client.execute_statement(
            Database=redshift_database,
            SecretArn=secret_arn,
            WorkgroupName=workgroup_name,
            Sql=dynamic_query
        )

        # Wait for the view creation to complete
        query_id = response['Id']
        status = redshift_data_client.describe_statement(Id=query_id)['Status']
        while status not in ["FINISHED", "FAILED", "ABORTED"]:
            time.sleep(1)
            status = redshift_data_client.describe_statement(Id=query_id)['Status']

        if status == "FINISHED":
            logger.info("Snapshot table created or replaced successfully.")
        else:
            failure_details = redshift_data_client.describe_statement(Id=query_id)
            logger.error(f"Error occurred while creating or replacing the snapshot table: {failure_details.get('Error')}")
            raise Exception(f"Error occurred while creating or replacing the snapshot table: {failure_details.get('Error')}")

    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise Exception(f"Error occurred: {e}")

    return {
        'statusCode': 200,
        'body': 'Snapshot table created or replaced successfully.',
    }
