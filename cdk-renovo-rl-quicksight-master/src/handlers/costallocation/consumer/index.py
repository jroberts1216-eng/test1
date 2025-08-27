import boto3
import json
import os

def handler(event, context):
    # Initialize QuickSight client
    quicksight_client = boto3.client('quicksight')

    # Process each message received from SQS
    for record in event['Records']:
        message = json.loads(record['body'])
        user_arn = message['userArn']
        tags_to_update = message['tagsToUpdate']

        # Apply the tags
        new_tags = [{'Key': key, 'Value': value} for key, value in tags_to_update.items()]
        if new_tags:
            quicksight_client.tag_resource(ResourceArn=user_arn, Tags=new_tags)
            print(f"Updated tags for user {message['userName']} in namespace {message['namespace']}.")

    return {
        'statusCode': 200,
        'body': json.dumps("Tagging completed for the batch.")
    }
