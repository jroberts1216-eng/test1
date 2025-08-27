import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import gs_now
import boto3

import re
import pandas as pd
import numpy as np

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
# Optimize the data movement from pandas to Spark DataFrame and back
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

tables = [
    {"db":"renovomaster" ,"table_name":"l_lookups","table_pk":"l_lookupkey","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"l_mfgmodalities","table_pk":"l_mfgmodalityid","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"l_pmschedules","table_pk":"l_pmschedid","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"l_setimetypesext","table_pk":"l_lookupkey","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"localpermissions","table_pk":"localpermissionid","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"mtfrequencychangequeue","table_pk":"id","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"mtmodels","table_pk":"mtmodelid","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"mtvendors","table_pk":"mtvendorid","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"organizations","table_pk":"orgid","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"sageregions","table_pk":"id","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"userorgs","table_pk":"id","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"mtdeviceacqpricing","table_pk":"id","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"users","table_pk":"userid","type":"incremental" },
    {"db":"renovomaster" ,"table_name":"mtdevices","table_pk":"mtdeviceid","type":"incremental" }
]

def validate_table(table_name,load_type="I"):
    
    if load_type=="F":
        return False
    
    AmazonRedshift_node1716754791668 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": f"SELECT COUNT(*) as count_table FROM pg_tables WHERE  tablename = '{table_name}' AND schemaname ='renovomaster'", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "redshift_con_cdk"}, transformation_ctx="AmazonRedshift_node1716754791668")
    
    spark_df = AmazonRedshift_node1716754791668.toDF()
    pandas_df = spark_df.toPandas()
    
    # Iterate over rows and process each row
    for index, row in pandas_df.iterrows():
        # Example: Print values or perform other operations
        if row['count_table'] == 1 :
            logger.info(f"{table_name} exists creating incremental logic")
            print(f"{table_name} exists creating incremental logic")
            return True
        else:
            logger.info(f"{table_name} do not exists or running full load logic")
            print(f"{table_name} do not exists or running full load logic")
            return False
    return False

def create_tmp_inc_table(db,table_name,table_pk):
    
    print(f"Starting Incremental logic for table:{table_name}")
    clean_table_name = f"{db}.{table_name}"
    inc_table_name = f"{table_name}_tmp_inc"
    full_inc_table_name = f"{db}.{inc_table_name}"
    
    inc_table_logic = f"SELECT tbl.*, case when tbl.ingestion_seq is not null and TRIM(tbl.ingestion_seq)<>'' then substring(tbl.ingestion_seq,0,17) else null end as ingestion_time_from_seq, case when tbl.ingestion_seq is not null and TRIM(tbl.ingestion_seq)<>'' then substring(tbl.ingestion_seq,17,len(tbl.ingestion_seq))::bigint else 0 end as ingestion_sequence FROM {db}_raw.{table_name} tbl WHERE tbl.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {db}.{table_name}) QUALIFY ROW_NUMBER() OVER(PARTITION BY tbl.{table_pk} ORDER BY tbl.INGESTION_TIMESTAMP DESC,ingestion_sequence DESC) =1"

    create_tmp_table_stmnt= f"CREATE TABLE {full_inc_table_name} AS\n{inc_table_logic}"
    print(create_tmp_table_stmnt)
    drop_tmp_table_stmnt= f"DROP TABLE IF EXISTS {full_inc_table_name}"
    print(drop_tmp_table_stmnt)

    spark_df_columns = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": f"select * from {clean_table_name} LIMIT 1", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "redshift_con_cdk"}, transformation_ctx="spark_df_columns")
    
    spark_df = spark_df_columns.toDF()
    pandas_df = spark_df.toPandas()
    update_stmnt = ""
    insert_col_stmnt = ""
    dtypes = {column: idx for idx, column in enumerate(pandas_df.columns)}
    max_position = max(dtypes.values())

    for key, value in dtypes.items():
        if key =='ingestion_sequence':
            update_stmnt = update_stmnt + f"{key} = {inc_table_name}.{key} "
            insert_col_stmnt = insert_col_stmnt + f"{inc_table_name}.{key}"
        else:
            update_stmnt = update_stmnt + f"{key} = {inc_table_name}.{key}, "
            insert_col_stmnt = insert_col_stmnt + f"{inc_table_name}.{key}, "

    merge_sql = f"MERGE INTO {clean_table_name} USING {full_inc_table_name} on {table_name}.{table_pk} = {inc_table_name}.{table_pk} WHEN MATCHED THEN UPDATE SET {update_stmnt} WHEN NOT MATCHED THEN INSERT VALUES ({insert_col_stmnt})"
    
    print(merge_sql)

    # Script generated for node Amazon Redshift
    AmazonRedshift_node1716838305686 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": f"select '{table_name}' as table_name, max(ingestion_timestamp) as last_update from \"dev\".\"{db}\".{table_name}", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "redshift_con_cdk"}, transformation_ctx="AmazonRedshift_node1716838305686")
    
    # Script generated for node Amazon Redshift
    AmazonRedshift_node1716838489827 = glueContext.write_dynamic_frame.from_options(frame=AmazonRedshift_node1716838305686, connection_type="redshift", connection_options={"postactions": f"BEGIN; MERGE INTO analytics.loading_logs USING analytics.loading_logs_temp_d2hlx1 ON loading_logs.table_name = loading_logs_temp_d2hlx1.table_name WHEN MATCHED THEN UPDATE SET table_name = loading_logs_temp_d2hlx1.table_name, last_update = loading_logs_temp_d2hlx1.last_update WHEN NOT MATCHED THEN INSERT VALUES (loading_logs_temp_d2hlx1.table_name, loading_logs_temp_d2hlx1.last_update); DROP TABLE analytics.loading_logs_temp_d2hlx1; END;", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "analytics.loading_logs_temp_d2hlx1", "connectionName": "redshift_con_cdk", "preactions": f"CREATE TABLE IF NOT EXISTS analytics.loading_logs (table_name VARCHAR, last_update TIMESTAMP); DROP TABLE IF EXISTS analytics.loading_logs_temp_d2hlx1; CREATE TABLE analytics.loading_logs_temp_d2hlx1 (table_name VARCHAR, last_update TIMESTAMP); {drop_tmp_table_stmnt}; {create_tmp_table_stmnt}; {merge_sql}; {drop_tmp_table_stmnt};"}, transformation_ctx="AmazonRedshift_node1716838489827")
    #{merge_sql};
    print(f"Incremental table loaded successfully")
   
def create_full_load_table(db,table_name,table_pk):
    
    table_logic = f"SELECT tbl.*, case when tbl.ingestion_seq is not null and TRIM(tbl.ingestion_seq)<>'' then substring(tbl.ingestion_seq,0,17) else null end as ingestion_time_from_seq, case when tbl.ingestion_seq is not null and TRIM(tbl.ingestion_seq)<>'' then substring(tbl.ingestion_seq,17,len(tbl.ingestion_seq))::bigint else 0 end as ingestion_sequence FROM {db}_raw.{table_name} tbl QUALIFY ROW_NUMBER() OVER(PARTITION BY tbl.{table_pk} ORDER BY tbl.INGESTION_TIMESTAMP DESC,ingestion_sequence DESC) =1"

    clean_table_name = f"{db}.{table_name}"
    create_tmp_table_stmnt= f"CREATE TABLE {clean_table_name} AS\n{table_logic}"
    
    
    AmazonRedshift_node1716844567969 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": f"select '{table_name}' as table_name", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "redshift_con_cdk"}, transformation_ctx="AmazonRedshift_node1716844567969")

    # Script generated for node Add Current Timestamp
    AddCurrentTimestamp_node1716853352573 = AmazonRedshift_node1716844567969.gs_now(colName="last_update")
    
    # Script generated for node Amazon Redshift
    AmazonRedshift_node1716838489827 = glueContext.write_dynamic_frame.from_options(frame=AddCurrentTimestamp_node1716853352573, connection_type="redshift", connection_options={"postactions": f"BEGIN; MERGE INTO analytics.loading_logs USING analytics.loading_logs_temp_d2hlx1 ON loading_logs.table_name = loading_logs_temp_d2hlx1.table_name WHEN MATCHED THEN UPDATE SET table_name = loading_logs_temp_d2hlx1.table_name, last_update = loading_logs_temp_d2hlx1.last_update WHEN NOT MATCHED THEN INSERT VALUES (loading_logs_temp_d2hlx1.table_name, loading_logs_temp_d2hlx1.last_update); DROP TABLE analytics.loading_logs_temp_d2hlx1; END;", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "analytics.loading_logs_temp_d2hlx1", "connectionName": "redshift_con_cdk", "preactions": f"CREATE TABLE IF NOT EXISTS analytics.loading_logs (table_name VARCHAR, last_update TIMESTAMP); DROP TABLE IF EXISTS analytics.loading_logs_temp_d2hlx1; CREATE TABLE analytics.loading_logs_temp_d2hlx1 (table_name VARCHAR, last_update TIMESTAMP); {create_tmp_table_stmnt};"}, transformation_ctx="AmazonRedshift_node1716838489827")

    print(f"Full table loaded successfully")

for table in tables:
    print(table['table_name'])
    if validate_table(table['table_name']):
        create_tmp_inc_table(table['db'],table['table_name'],table['table_pk'])
    else:
        create_full_load_table(table['db'],table['table_name'],table['table_pk'])


job.commit()