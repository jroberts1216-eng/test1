import boto3
import json
import os

def handler(event, context):
    sqs_queue_url = os.getenv('SQS_QUEUE_URL')
    tag_json = os.getenv('TAGS_JSON')  # JSON string of tags

    required_tags = json.loads(tag_json)

    sts_client = boto3.client('sts')
    quicksight_client = boto3.client('quicksight')
    sqs_client = boto3.client('sqs')
    
    account_id = sts_client.get_caller_identity()['Account']

    namespaces_response = quicksight_client.list_namespaces(AwsAccountId=account_id)
    namespaces = [ns['Name'] for ns in namespaces_response.get('Namespaces', [])]

    for namespace in namespaces:
        print(f"Processing users in namespace: {namespace}")
        response = quicksight_client.list_users(AwsAccountId=account_id, Namespace=namespace)
        users = response.get('UserList', [])

        for user in users:
            user_arn = user['Arn']
            tags = quicksight_client.list_tags_for_resource(ResourceArn=user_arn)
            existing_tags = {tag['Key']: tag['Value'] for tag in tags.get('Tags', [])}

            tag_diff = {key: val for key, val in required_tags.items() if existing_tags.get(key) != val}
            if tag_diff:
                message_body = json.dumps({
                    'userArn': user_arn,
                    'userName': user['UserName'],
                    'namespace': namespace,
                    'tagsToUpdate': tag_diff
                })
                sqs_client.send_message(QueueUrl=sqs_queue_url, MessageBody=message_body)

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed users across {len(namespaces)} namespaces.")
    }
