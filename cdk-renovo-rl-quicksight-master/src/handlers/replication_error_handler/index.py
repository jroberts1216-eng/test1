import json
import os
import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dms_client = boto3.client('dms')
sns_client = boto3.client('sns')

def parse_arn_and_construct_url(arn, name):
    # Split the ARN into its components
    parts = arn.split(':')

    # Extract region and task name
    region = parts[3]

    # Construct the URL
    url = f"https://{region}.console.aws.amazon.com/dms/v2/home?region={region}#taskDetails/{name}"

    return url

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))

    # Get environment variables
    SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

    # Ensure required env variables are set
    if not all([SNS_TOPIC_ARN]):
        raise ValueError("SNS_TOPIC_ARN environment variable must be set")

    successful_messages = 0
    failed_messages = 0
    
    # Process each SQS record
    for record in event['Records']:
        try:
            # Parse the SQS message
            message = json.loads(record['body'])

            # Extract DMS Task State details and message
            dms_state = message.get("detail-type")
            detail_message = message["detail"].get("detailMessage", "No detail message provided.")
            
            if dms_state != "DMS Replication Task State Change":
                logger.info("Not a DMS Replication Task State Change. Skipping record.")
                continue
            
            # Get task ARN directly from the message
            task_arn = message["resources"][0]
            
            # Use boto3 to get the task name using the task ARN
            task_info = dms_client.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [task_arn]
                    },
                ]
            )

            # Extract task name
            task_name = task_info['ReplicationTasks'][0]['ReplicationTaskIdentifier']

            url = parse_arn_and_construct_url(task_arn, task_name)

            # Construct the SNS messages
            subject = f"{task_name} DMS task failed"
            short_message = f"DMS task {task_name} has failed"
            long_message = f"{short_message}. Please login to the console and see {url} for more details. Error message: {detail_message}"

            sns_message = {
                'default': long_message,
                'sms': short_message,
                'email': long_message
            }

            # Publish the message to SNS
            response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=json.dumps(sns_message),
                Subject=subject,
                MessageStructure='json'
            )
            logger.info(f"Message sent to SNS for task {task_name} with MessageId: {response['MessageId']}")
            successful_messages += 1
            
        except Exception as e:
            error_msg = f"Failed to process record: {str(e)}"
            logger.error(error_msg)
            failed_messages += 1

    # Return appropriate status based on results
    if failed_messages == 0:
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully processed {successful_messages} messages')
        }
    elif successful_messages > 0:
        return {
            'statusCode': 207,  # Partial success
            'body': json.dumps(f'Processed {successful_messages} successfully, {failed_messages} failed')
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Failed to process all {failed_messages} messages')
        }
