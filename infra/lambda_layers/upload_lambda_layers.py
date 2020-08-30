import logging
import sys
import urllib
from io import BytesIO

import boto3
import botocore

LAYERS_URLS = [
    "https://github.com/awslabs/aws-data-wrangler/releases/download/1.6.0/awswrangler-layer-1.6.0-py3.6.zip"
]


def main():
    setup_logging()
    bucket_name = sys.argv[1]
    s3_client = boto3.client(service_name='s3')

    for url in LAYERS_URLS:
        file_name = url.rsplit(sep='/', maxsplit=1)[1]
        s3_object_key = f'lambda_layers/{file_name}'
        logging.info(f'Verifying if lambda layer {file_name} exists....')
        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_object_key)
            logging.info('Lambda layer already exists.')
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == '404':
                logging.info('Lambda layer does not exists. Downloading....')
                response = urllib.request.urlopen(url)
                logging.info('Uploading Lambda layer to s3....')
                s3_client.put_object(Body=BytesIO(response.read()),
                                     Bucket=bucket_name,
                                     Key=s3_object_key)
                logging.info('Upload completed.')
            else:
                logging.error('Lambda layer process failed.')
                raise err


def setup_logging():
    root = logging.getLogger()
    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)
    logging.basicConfig(
        format=
        '[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s]   %(message)s',
        level='INFO')


if __name__ == '__main__':
    main()
