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
import time


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()

spark = glueContext.spark_session
job = Job(glueContext)

# Source Databases includes clRenovo and Cl% 
source_db = 'clrenovo-db'
# Job Tables
'''
tables_to_transform = [{ "table_name":"regions","table_pk":"regionid","type":"full" }]
'''
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
    {"table_name":"inventoryitems","table_pk":"inventoryitemid","type":"full" },
    {"table_name":"po_vpliadjustments","table_pk":"povplineitemid","type":"full" },
    {"table_name":"vsc_liequipment","table_pk":"vsclineitemid","type":"full" },
    {"table_name":"regions","table_pk":"regionid","type":"full" },
    {"table_name":"localpermission_usersettings","table_pk":"localPermissionID,userid,locationkey","type":"full" },
    {"table_name":"locationrole_permissions","table_pk":"locationroleid,localPermissionID","type":"full" },
    {"table_name":"locationrole_users","table_pk":"locationroleid,userid,locationkey","type":"full" },
    {"table_name":"locationroles","table_pk":"locationRoleID","type":"full" },
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

job.init(args['JOB_NAME'], args)

import boto3
client = boto3.client('glue')

# Variables for the job: 
glueJobName = "Inc_cl_tables_cdk"
glueJobNameRenovoMaster = "Inc_renovo_tables_cdk"
glueJobNameRefreshViews = "RefreshMaterializedViews_cdk"

# Define Lambda function
def trigger_job(job_to_run):
    response = client.start_job_run(
        JobName = glueJobName,
        Arguments={
         '--table_name': str(job_to_run['table_name']),
         '--table_pk': str(job_to_run['table_pk']),
         '--type': str(job_to_run['type']),
         '--source_db':str(source_db)
        })
    print('## STARTED GLUE JOB: ' + glueJobName)
    print('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response['JobRunId']
    
def trigger_no_params(jobName):
    response = client.start_job_run(
        JobName = jobName)
    print('## STARTED GLUE JOB: ' + jobName)
    print('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return response['JobRunId']

JobsRenovoRunning =[]
JobsRenovoRunning.append(trigger_no_params(glueJobNameRenovoMaster))
JobsRenovoExpected = len(JobsRenovoRunning)
JobsRenovoEvaluated = 0


JobsRunning =[]
for job_to_run in tables_to_transform:
    JobsRunning.append(trigger_job(job_to_run))

JobsExpected = len(JobsRunning)
JobsEvaluated = 0


while JobsRenovoEvaluated<len(JobsRenovoRunning):
    response = client.get_job_run(JobName=glueJobNameRenovoMaster,RunId=JobsRenovoRunning[JobsRenovoEvaluated])
    if response['JobRun']['JobRunState']=='SUCCEEDED':
        #JobsRunning.pop(job)
        print(f"{JobsRenovoEvaluated+1}/{JobsRenovoExpected} JobId:{JobsRenovoRunning[JobsRenovoEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']} and Completed: {response['JobRun']['CompletedOn']}")
        JobsRenovoEvaluated=JobsRenovoEvaluated+1
    elif response['JobRun']['JobRunState']=='FAILED':
        print(f"{JobsRenovoEvaluated+1}/{JobsRenovoExpected} JobId:{JobsRenovoRunning[JobsRenovoEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']}")
        JobsRenovoEvaluated=JobsRenovoEvaluated+1
    else:
        print(f"{JobsRenovoEvaluated+1}/{JobsRenovoExpected} JobId:{JobsRenovoRunning[JobsRenovoEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']}")
        time.sleep(30)


while JobsEvaluated<len(JobsRunning):
    response = client.get_job_run(JobName=glueJobName,RunId=JobsRunning[JobsEvaluated])
    if response['JobRun']['JobRunState']=='SUCCEEDED':
        #JobsRunning.pop(job)
        print(f"{JobsEvaluated+1}/{JobsExpected} JobId:{JobsRunning[JobsEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']} and Completed: {response['JobRun']['CompletedOn']}")
        JobsEvaluated=JobsEvaluated+1
    elif response['JobRun']['JobRunState']=='FAILED':
        print(f"{JobsEvaluated+1}/{JobsExpected} JobId:{JobsRunning[JobsEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']}")
        JobsEvaluated=JobsEvaluated+1
    else:
        print(f"{JobsEvaluated+1}/{JobsExpected} JobId:{JobsRunning[JobsEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']}")
        time.sleep(30)



JobsRunning =[]
JobsRunning.append(trigger_no_params(glueJobNameRefreshViews))
JobsExpected = len(JobsRunning)
JobsEvaluated = 0

while JobsEvaluated<len(JobsRunning):
    response = client.get_job_run(JobName=glueJobNameRefreshViews,RunId=JobsRunning[JobsEvaluated])
    if response['JobRun']['JobRunState']=='SUCCEEDED':
        #JobsRunning.pop(job)
        print(f"{JobsEvaluated+1}/{JobsExpected} JobId:{JobsRunning[JobsEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']} and Completed: {response['JobRun']['CompletedOn']}")
        JobsEvaluated=JobsEvaluated+1
    elif response['JobRun']['JobRunState']=='FAILED':
        print(f"{JobsEvaluated+1}/{JobsExpected} JobId:{JobsRunning[JobsEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']}")
        JobsEvaluated=JobsEvaluated+1
    else:
        print(f"{JobsEvaluated+1}/{JobsExpected} JobId:{JobsRunning[JobsEvaluated]} Status:{response['JobRun']['JobRunState']} Started: {response['JobRun']['StartedOn']}")
        time.sleep(60)

job.commit()