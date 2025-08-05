import os
import boto3
import json

sns_client = boto3.client('sns', region_name='us-east-1')
ERROR_SNS_TOPIC_ARN = os.environ['ERROR_SNS_TOPIC_ARN']


def notify_error(message):
    return sns_client.publish(
        TopicArn=ERROR_SNS_TOPIC_ARN,
        Subject="Error RedNotarial: fill_forms",
        Message=message
    )
