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
    "analytics.clusers",
    "analytics.vm_orglookups",
   "analytics.dim_purchase_orders",
   "analytics.dim_se_assigned",
   "ANALYTICS.dim_se_notes",
   "analytics.vi_sm_status_dashboard_detail",
   "ANALYTICS.vi_dm_list",
   "analytics.vFacilityManagementRoles",
   "analytics.vi_po_candidates_closure",
   "analytics.vi_purchase_order"
]

def update_views(list_tables):
    update_merg = "; REFRESH MATERIALIZED VIEW ".join(list_tables)
    update_merg = f"REFRESH MATERIALIZED VIEW {update_merg}"
    
    from awsglue.dynamicframe import DynamicFrame

    df_tables_to_update = pd.DataFrame(data=list_tables,columns=['table_name'])

    AmazonRedshift_node1716844567969 = DynamicFrame.fromDF(spark.createDataFrame(df_tables_to_update), glueContext, "AmazonRedshift_node1716844567969")
    
    #AmazonRedshift_node1716844567969 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"sampleQuery": f"select 'materialized views refreshed at ' as table_name", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "connectionName": "redshift_con_cdk"}, transformation_ctx="AmazonRedshift_node1716844567969")

    # Script generated for node Add Current Timestamp
    AddCurrentTimestamp_node1716853352573 = AmazonRedshift_node1716844567969.gs_now(colName="last_update")
    
    # Script generated for node Amazon Redshift
    AmazonRedshift_node1716838489827 = glueContext.write_dynamic_frame.from_options(frame=AddCurrentTimestamp_node1716853352573, connection_type="redshift", connection_options={"postactions": f"BEGIN; MERGE INTO analytics.loading_logs USING analytics.loading_logs_temp_d2hlx1 ON loading_logs.table_name = loading_logs_temp_d2hlx1.table_name WHEN MATCHED THEN UPDATE SET table_name = loading_logs_temp_d2hlx1.table_name, last_update = loading_logs_temp_d2hlx1.last_update WHEN NOT MATCHED THEN INSERT VALUES (loading_logs_temp_d2hlx1.table_name, loading_logs_temp_d2hlx1.last_update); DROP TABLE analytics.loading_logs_temp_d2hlx1; END;", "redshiftTmpDir": "s3://aws-glue-assets-719035735300-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "analytics.loading_logs_temp_d2hlx1", "connectionName": "redshift_con_cdk", "preactions": f"CREATE TABLE IF NOT EXISTS analytics.loading_logs (table_name VARCHAR, last_update TIMESTAMP); DROP TABLE IF EXISTS analytics.loading_logs_temp_d2hlx1; CREATE TABLE analytics.loading_logs_temp_d2hlx1 (table_name VARCHAR, last_update TIMESTAMP); {update_merg};"}, transformation_ctx="AmazonRedshift_node1716838489827")


update_views(tables)

job.commit()