import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Get parameters from script arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name', 'table_name', 'primary_key', 'env_name', 'connection_name', 'redshift_tmp_dir'])
job_name = args['JOB_NAME']
env_name = args['env_name']
database_name = args['database_name']
database_full_name = f"{env_name}-{database_name}"
table_name = args['table_name']
table_full_name = f"full_load_{table_name}"
primary_key = args['primary_key']
connection_name = args['connection_name']
redshift_tmp_dir = args['redshift_tmp_dir']

# Initialize Glue context and job
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(job_name, args)

logger = glue_context.get_logger()

logger.info(f"Connecting to {database_full_name} for table {table_full_name}")
# Create DynamicFrame from AWS Glue Data Catalog
dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=database_full_name,
    table_name=table_full_name,
    transformation_ctx=f"dynamic_frame_{table_full_name}"
)

target = f"{database_name}.{table_full_name}"
source = f"{database_name}.{table_full_name}_temp"

# Get field names from the dynamic frame
field_names_list = [field.name for field in dynamic_frame.schema().fields]
field_names = ", ".join(field_names_list)
update_set = ", ".join(f"{field} = {source}.{field}" for field in field_names_list  if field != primary_key)
insert_values = ", ".join(f"{source}.{field}" for field in field_names_list)

# Define Redshift connection options
connection_options = {
    "postactions": f"""
        BEGIN;
        MERGE INTO {target}
        USING {source}
        ON {target}.{primary_key} = {source}.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({field_names})
            VALUES ({insert_values});
        DROP TABLE {database_name}.{table_full_name}_temp;
        END;
        DROP TABLE IF EXISTS {database_name}.{table_name}_temp;
    """,
    "redshiftTmpDir": redshift_tmp_dir,
    "useConnectionProperties": "true",
    "dbtable": f"{database_name}.{table_full_name}_temp",
    "connectionName": connection_name,
    "preactions": f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_full_name} (LIKE {database_name}_spectrum.{table_full_name} INCLUDING DEFAULTS);
        DROP TABLE IF EXISTS {database_name}.{table_name}_temp;
        CREATE TABLE {database_name}.{table_name}_temp (LIKE {database_name}_spectrum.{table_full_name} INCLUDING DEFAULTS);
    """
}

logger.info(f"Loading data to redshift for {database_full_name} for table {table_full_name}")
# Write DynamicFrame to Amazon Redshift
glue_context.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options=connection_options,
    transformation_ctx=f"redshift_{table_full_name}"
)

# Commit the job
job.commit()
