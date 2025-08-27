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

# Define Redshift connection options
#
# Preactions utilize LIKE to create tables that use the crawled schema from the table
# available in Redshift Spectrum. This is done to ensure that the tables have the same
# schema and to avoid the need to specify the schema in this code which makes it reusable
connection_options = {
    "postactions": f"""
        BEGIN;
        MERGE INTO {database_name}.{table_full_name} AS target
        USING {database_name}.{table_full_name}_temp AS source
        ON target.{primary_key} = source.{primary_key}
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *;
        DROP TABLE {database_name}.{table_full_name}_temp;
        END;
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
