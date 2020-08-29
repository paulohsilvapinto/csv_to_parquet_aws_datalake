'''
    About: Lambda used to configure S3 Event Notifications on S3 Raw Bucket.
'''

import json
import boto3
import cfnresponse
import logging
import os

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
s3 = boto3.resource('s3')


# main function
def handler(event, context):
    setup_logging()
    logging.info(f'Received event: {json.dumps(event, indent=2)}')

    responseData = {}
    try:
        RequestType = event['RequestType']
        properties = event['ResourceProperties']
        Bucket = properties['Bucket']

        logging.info(f'Request Type: {RequestType}')

        if RequestType == 'Delete':
            delete_notification(Bucket)

        elif RequestType in ['Create', 'Update']:
            LambdaArn = properties['LambdaArn']
            add_notification(LambdaArn, Bucket)
            responseData = {'Bucket': Bucket}
        responseStatus = 'SUCCESS'

    except Exception as e:
        logging.error('Failed to process:', e)
        responseStatus = 'FAILED'
        responseData = {'Failed': 'Unable to complete the request.'}
    cfnresponse.send(event, context, responseStatus, responseData)


def setup_logging():
    root = logging.getLogger()
    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)
    logging.basicConfig(
        format=
        '[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s',
        level=LOG_LEVEL)


# creates s3 event notifications on s3 raw bucket
def add_notification(LambdaArn, Bucket):
    bucket_notification = s3.BucketNotification(Bucket)
    bucket_notification.put(
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [{
                'LambdaFunctionArn': LambdaArn,
                'Events': ['s3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [{
                            'Name': 'prefix',
                            'Value': 'csv_to_analytics/'
                        }, {
                            'Name': 'suffix',
                            'Value': '.csv'
                        }]
                    }
                }
            }]
        })
    logging.info('Put event notification request completed.')


# deletes s3 event notifications from s3 raw bucket
def delete_notification(Bucket):
    bucket_notification = s3.BucketNotification(Bucket)
    bucket_notification.put(NotificationConfiguration={})
    logging.info('Delete event notification request completed.')
