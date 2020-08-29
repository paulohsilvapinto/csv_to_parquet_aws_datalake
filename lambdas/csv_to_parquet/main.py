import os
import logging
import pandas as pd
import re
from datetime import datetime
import boto3
from ast import literal_eval
import awswrangler as wr

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
TARGET_S3_BUCKET = os.getenv("TARGET_S3_BUCKET")
TARGET_GLUE_DATABASE = os.getenv("TARGET_GLUE_DATABASE")


def handler(event, context):
    setup_logging()
    s3_objects = get_s3_objects_path(event)

    for s3_object in s3_objects:
        s3_source_path = s3_object['object_path']
        s3_object_meta = get_s3_object_metadata(s3_object)
        partition_cols = get_partition_cols(s3_object_path=s3_source_path,
                                            s3_object_meta=s3_object_meta)

        df = read_csv(s3_source_path=s3_source_path)
        if not df.empty:
            df = add_load_metadata_to_df(df, s3_source_path=s3_source_path)
            df = normalize_column_name(df)
            df['observation_date'] = pd.to_datetime(
                df['observation_date']).dt.date
            save_to_s3(dataframe=df,
                       table_name=s3_object['target_table'],
                       partition_cols=partition_cols)


def setup_logging():
    root = logging.getLogger()
    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)
    logging.basicConfig(
        format=
        '[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s] %(message)s',
        level=LOG_LEVEL)


def get_s3_objects_path(event):
    logging.info(f'Event: {str(event)}')

    s3_objects = []
    for record in event.get('Records', {}):
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        s3_objects.append({
            'object_path': f"s3://{bucket_name}/{object_key}",
            'object_bucket': bucket_name,
            'object_key': object_key,
            'target_table': f'tb_{object_key.split("/")[1]}'
        })

    return s3_objects


def get_s3_object_metadata(s3_object):
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=s3_object['object_bucket'],
                              Key=s3_object['object_key'])
    metadata = response.get('Metadata')
    logging.info('Metadata: {}'.format(metadata))

    return metadata


def get_partition_cols(s3_object_path, s3_object_meta):
    partition_cols = s3_object_meta.get('partition-cols')

    if partition_cols:

        try:
            partition_cols = literal_eval(partition_cols)
        except Exception:
            pass

        if isinstance(partition_cols, list):
            partition_cols = [_normalize_name(col) for col in partition_cols]
        elif isinstance(partition_cols, str):
            partition_cols = [_normalize_name(partition_cols)]
        else:
            raise Exception(
                f'Invalid partition-cols metadata for object {s3_object_path}. Please specify either a list or a string.'
            )

    logging.info('Partition Columns: {}'.format(partition_cols))
    return partition_cols


def read_csv(s3_source_path):
    try:
        df = pd.read_csv(s3_source_path)
    except Exception as e:
        logging.error(f'Failed to read csv {s3_source_path} on S3. {e}')
        raise e

    return df


def add_load_metadata_to_df(dataframe, s3_source_path):
    dataframe['dl_creation_date'] = datetime.today().date()
    dataframe['dl_source_file'] = s3_source_path
    return dataframe


def _normalize_name(name):
    name = name.replace(' ', '_')
    name = name.replace('-', '_')
    name = name.replace('.', '_')
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    name = name.lower()
    return re.sub(r'(_)\1+', '\\1', name)


def normalize_column_name(dataframe):
    columns = list(dataframe.columns)
    dataframe.columns = [_normalize_name(column) for column in columns]
    return dataframe


def save_to_s3(dataframe, table_name, partition_cols, compression='snappy'):
    dest_path = f's3://{TARGET_S3_BUCKET}/databases/{TARGET_GLUE_DATABASE}/{table_name}/'
    try:
        wr.s3.to_parquet(df=dataframe,
                         path=dest_path,
                         compression='snappy',
                         dataset=True,
                         partition_cols=partition_cols,
                         mode='overwrite_partitions',
                         database=TARGET_GLUE_DATABASE,
                         table=table_name)
    except Exception as e:
        logging.error(f'Failed to save {dest_path} to S3. {e}')
        raise e


if __name__ == '__main__':
    event = {
        'Records': [{
            's3': {
                'bucket': {
                    'name': 'phsp-dlg-datalake-raw-dev'
                },
                'object': {
                    'key': 'csv_to_analytics/weather/weather.20160201.csv'
                }
            }
        }]
    }
    handler(event, {})
