import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from datetime import datetime 
import json

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    
    
args = getResolvedOptions(sys.argv, ['JOB_NAME','table_name','table_pk','type','source_db'])
sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

spark = glueContext.spark_session
job = Job(glueContext)

# Source Databases includes clRenovo and Cl% 
source_db = args['source_db'].split(',')
# Job Tables

tables_to_transform = [
     {
        'table_name':args['table_name'],
        'table_pk':args['table_pk'],
        'type':args['type']
     }
    ]
    
'''
source_db = ['clrenovo']
# Job Tables
tables_to_transform = [
{"table_name":"clients","table_pk":"clientid","type":"full" }
]
tables_to_transform = [
{"table_name":"clients","table_pk":"clientid","type":"full" },
{"table_name":"clientsystems","table_pk":"systemid","type":"full" },
{"table_name":"coverage_coveragedetailstemplates","table_pk":"coverage_coveragedetailstemplateid","type":"full" },
{"table_name":"coverage_coveragespecification","table_pk":"coverage_coveragespecid","type":"full" },
{"table_name":"documents","table_pk":"documentid","type":"full" },
{"table_name":"facilities","table_pk":"facilityid","type":"full" },
{"table_name":"facility_costcenters","table_pk":"facility_costcenterid","type":"full" },
{"table_name":"facility_equipmentbudget","table_pk":"facilityequipmentbudgetid","type":"full" },
{"table_name":"facility_equipment","table_pk":"facility_equipmentid","type":"full" },
{"table_name":"ismsystems","table_pk":"ismsystemid","type":"full" },
{"table_name":"l_equipmentstatus","table_pk":"l_equipmentstatusid","type":"full" },
{"table_name":"l_sediagnosis","table_pk":"sediagnid","type":"full" },
{"table_name":"l_sespecialconditions","table_pk":"sespeccondid","type":"full" },
{"table_name":"l_sestatus","table_pk":"sestatusid","type":"full" },
{"table_name":"l_seresolutions","table_pk":"seresid","type":"full" },
{"table_name":"l_sesymptoms","table_pk":"sesymptid","type":"full" },
#{"table_name":"locations","table_pk":"locationkey","type":"full" },
{"table_name":"locationrole_permissions","table_pk":"locationroleid,localPermissionID","type":"full" },
{"table_name":"locationrole_users","table_pk":"locationroleid,userid,locationkey","type":"full" },
{"table_name":"locationroles","table_pk":"locationRoleID","type":"full" },
#{"table_name":"locationtree","table_pk":"rootlocationkey,locationkey","type":"full" },
{"table_name":"mtclaem","table_pk":"mtclaemid","type":"full" },
{"table_name":"mtcldevices","table_pk":"mtcldeviceid","type":"full" },
{"table_name":"mtcldevicetemps","table_pk":"mtcldevicetempid","type":"full" },
{"table_name":"mtclmodels","table_pk":"mtclmodelid","type":"full" },
{"table_name":"po_lineitems","table_pk":"polineitemid","type":"full" },
{"table_name":"po_purchaseorders","table_pk":"purchaseorderid","type":"full" },
{"table_name":"po_vendorpayments","table_pk":"povendorpaymentid","type":"full" },
{"table_name":"po_vplineitems","table_pk":"povplineitemid","type":"full" },
{"table_name":"po_liexchanges","table_pk":"polineitemid","type":"full" },
{"table_name":"se_assignedusers","table_pk":"se_assigneduserid","type":"full" },
{"table_name":"se_partsused","table_pk":"se_partsusedid","type":"full" },
{"table_name":"se_serviceevents","table_pk":"serviceeventid","type":"full" },
{"table_name":"se_servicenotes","table_pk":"se_servicenoteid","type":"full" },
{"table_name":"se_specialconditions","table_pk":"se_serviceeventid","type":"full" },
{"table_name":"se_time","table_pk":"se_timeid","type":"full" },
{"table_name":"vendors2","table_pk":"vendorid","type":"full" },
{"table_name":"vendorinvoices","table_pk":"vendorinvoiceid","type":"full" },
{"table_name":"vs_vendorsubcontract","table_pk":"vendorsubcontractid","type":"full" },
{"table_name":"vsc_lineitems","table_pk":"vsclineitemid","type":"full" }
]
'''

job.init(args['JOB_NAME'], args)



# Script generated for node Amazon S3
additional_options = {"write.parquet.compression-codec": "gzip"}

table_location = 's3://rl-quicksight-719035735300-us-east-1/renovolive/unifieddb'

# List of tables on destination db
#spark.sql("CREATE DATABASE IF NOT EXISTS unifieddb")
try:
    tables_collection = spark.catalog.listTables("unifieddb")
    table_names_in_db = [table.name for table in tables_collection]
except:
    tables_collection = []
    table_names_in_db =[]
'''
table_location = 's3://caylent-s3-test/clean/unified_db'

# List of tables on destination db
tables_collection = spark.catalog.listTables("unified_db")
table_names_in_db = [table.name for table in tables_collection]
'''
print(f"Table List {tables_collection}")

def add_additional_columns(glueContext,df,database):
    print('## STARTING ADD COLUMNS')
    sqlQuery = f"""
    SELECT '{database}' as DB, *, case when ingestion_seq is not null and TRIM(ingestion_seq)<>'' then substring(ingestion_seq,0,17) else null end as ingestion_time_from_seq, case when ingestion_seq is not null and TRIM(ingestion_seq)<>'' then substring(ingestion_seq,17,length(ingestion_seq)) else 0 end as ingestion_sequence, date_trunc('day',INGESTION_TIMESTAMP) as partition_date from source_to_db
    """
    return sparkSqlQuery(glueContext, query = sqlQuery, mapping = {"source_to_db":df},transformation_ctx = f"add_additional_columns_{database}")
    
def dedup_records(glueContext,df,table):
    print('## STARTING DEDUP RECORDS')
    sqlQuery = f"""
    WITH cte AS (
        select *,
        ROW_NUMBER() OVER(PARTITION BY db,{table['table_pk']} ORDER BY INGESTION_TIMESTAMP DESC,ingestion_sequence DESC) as row_number_ctx
        from dedup_records
    )
    select * from cte where row_number_ctx = 1
    ORDER BY DB,{table['table_pk']} ASC
    """
    tmp_df = sparkSqlQuery(glueContext, query = sqlQuery, mapping = {"dedup_records":df},transformation_ctx = f"dedup_records_{table['table_name']}")
    print('## END DEDUP RECORDS')
    return DropFields.apply(frame=tmp_df, paths=["row_number_ctx"], transformation_ctx=f"DropFields_dedup_{table['table_name']}")

def filter_incremental(glueContext,df,table,db):
    sqlQuery = f"""
    WITH cte AS (
        select *,
        ROW_NUMBER() OVER(PARTITION BY db,{table['table_pk']} ORDER BY INGESTION_TIMESTAMP DESC,ingestion_sequence DESC) as row_number_ctx
        from incremental_db
        where INGESTION_TIMESTAMP >(select MAX(INGESTION_TIMESTAMP) from glue_catalog.unifieddb.{table['table_name']} WHERE DB = '{db}')
    )
    select * from cte where row_number_ctx = 1
    ORDER BY DB,{table['table_pk']} ASC
    """
    tmp_df = sparkSqlQuery(glueContext, query = sqlQuery, mapping = {"incremental_db":df},transformation_ctx = f"filter_incremental_{table['table_name']}")

    return DropFields.apply(frame=tmp_df, paths=["row_number_ctx"], transformation_ctx=f"DropFields_{table['table_name']}")

def get_raw_data(table,is_incremental=False):
    print(f"## Getting data for {table['table_name']}")
    df_unified = None
    
    for db in source_db:
        
        if is_incremental:
            print('## START QUERYING GLUE FOR INCREMENTAL DATA')
            df_raw_source = glueContext.create_dynamic_frame.from_catalog(database=db, table_name=f"{table['table_name']}",sample_query = f"select * from {table['table_name']} WHERE INGESTION_TIMESTAMP>='{datetime.utcnow().strftime('%Y-%m-%d')}'", transformation_ctx="df_raw_source")
            if df_raw_source.count() >0:
                #print(f"Count of Records : {str(tmp_df_val)}")
                #df_raw_source.show(1)
                df_raw_source = add_additional_columns(glueContext,df_raw_source,db)
                df_raw_source = filter_incremental(glueContext,df_raw_source,table,db)
            else:
                print('## NO DATA RETURNED')
        else:
            print(f'## START QUERYING GLUE FOR FULL DATA')
            df_raw_source = glueContext.create_dynamic_frame.from_catalog(database=db, table_name=f"{table['table_name']}", transformation_ctx="df_raw_source")
            if df_raw_source.count() >0:
                df_raw_source = add_additional_columns(glueContext,df_raw_source,db)
                df_raw_source = dedup_records(glueContext,df_raw_source,table)
            else:
                print('## NO DATA RETURNED')
                
                
        if df_unified is None:
            df_unified = df_raw_source.toDF()
        else:
            df_unified.union(df_raw_source.toDF())
    return df_unified

def run_full_load(table):
    df_source = get_raw_data(table,is_incremental=False)
    if not df_source.rdd.isEmpty():
        df_source.sortWithinPartitions("db","partition_date") \
            .writeTo(f"glue_catalog.unifieddb.{table['table_name']}") \
            .tableProperty("format-version", "2") \
            .tableProperty("location", f"{table_location}/{table['table_name']}") \
            .options(**additional_options) \
            .partitionedBy("db","partition_date").createOrReplace()

def run_incremental(table):
    df_source = get_raw_data(table,is_incremental=True)
    if not df_source.rdd.isEmpty():
        print(f"Count of Records to Insert: {df_source.rdd.count()}")
        inc_table_name = f"tmp_inc_{table['table_name']}"
        df_source.createOrReplaceTempView(inc_table_name)
        table_name = f"{table['table_name']}"
        
        merge_sql_1 = f"MERGE INTO glue_catalog.unifieddb.{table['table_name']} USING {inc_table_name} on {table_name}.{table['table_pk']} = {inc_table_name}.{table['table_pk']} AND  {table_name}.DB = {inc_table_name}.DB WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        result = spark.sql(merge_sql_1)

def transform_table(table):
    table_exists = table['table_name'] in table_names_in_db
    if table['type']=='full':
        print(f"Starting {table['table_name']} full load")
        run_full_load(table)
    else:
        if table_exists:
            print(f"Starting {table['table_name']} incremental load")
            run_incremental(table)
        else:
            print(f"Starting {table['table_name']} incremental load")
            run_full_load(table)
    
    print(f"Table {table['table_name']} loaded")

for tables_job in tables_to_transform:
    transform_table(tables_job)

job.commit()