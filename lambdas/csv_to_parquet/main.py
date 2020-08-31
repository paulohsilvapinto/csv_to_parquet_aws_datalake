'''
    About: Generic Lambda function that runs whenever it receives s3 events.
           The trigger event is generated every time a CSV file is created in "folder"
           s3://[S3RawBucket]/csv_to_parquet/[data_desc_folder]/
'''

import os
import logging
import pandas as pd
import re
import numpy as np
import boto3

from datetime import datetime
from ast import literal_eval
import awswrangler as wr

# GLOBAL VARIABLES
# gets environment variables
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
EXECUTION_MODE = os.getenv('EXECUTION_MODE',
                           'local')  # accepted values: cloud or local

TARGET_S3_BUCKET = os.getenv('TARGET_S3_BUCKET')
TARGET_GLUE_DATABASE = os.getenv('TARGET_GLUE_DATABASE')
SNS_TOPIC_NAME = os.getenv('SNS_TOPIC_NAME')

# aws sns topic arn is set by application
SNS_TOPIC_ARN = ''

# LOCAL_CSV_FILE_PATH used only for running the script locally
LOCAL_CSV_FILE_PATH = os.path.join(
    os.path.dirname(__file__),
    '..\\..\\test-data\\weather\\incoming_data\\weather.20160201.csv')


# main function
def handler(event, context):
    setup_logging()
    build_sns_topic_arn(context)
    s3_objects = parse_event(event)

    for s3_object in s3_objects:
        source_file_path = get_source_file_path(s3_object)
        logging.info(f'#--- Starting processing file {source_file_path}. ---#')
        s3_object_meta = get_s3_object_metadata(s3_object)
        partition_cols = get_partition_cols(source_file_path=source_file_path,
                                            s3_object_meta=s3_object_meta)

        df = read_csv(source_file_path=source_file_path,
                      event=event,
                      s3_object_meta=s3_object_meta)
        if not df.empty:
            df = cast_df_columns(dataframe=df,
                                 source_file_path=source_file_path,
                                 s3_object_meta=s3_object_meta)
            df = str_columns_to_upper(df, s3_object_meta)
            df = add_etl_metadata_to_df(df, source_file_path=source_file_path)
            df = normalize_column_name(df)
            df = replace_nan_values(df)
            save_as_parquet(dataframe=df,
                            s3_object=s3_object,
                            partition_cols=partition_cols,
                            event=event,
                            s3_object_meta=s3_object_meta)
            publish_success_to_sns(s3_object)
            logging.info(
                f'#--- Finished loading file {source_file_path}. ---#')
        else:
            logging.info(
                f'#--- File {source_file_path} is empty. No data was loaded. ---#'
            )


def setup_logging():
    root = logging.getLogger()
    if root.handlers:
        for h in root.handlers:
            root.removeHandler(h)
    logging.basicConfig(
        format=
        '[%(asctime)s][%(levelname)s][%(name)s][%(funcName)s]   %(message)s',
        level=LOG_LEVEL)


def _is_cloud_execution_mode():
    if EXECUTION_MODE == 'cloud':
        return True
    return False


def _is_local_execution_mode():
    if EXECUTION_MODE == 'local':
        return True
    return False


# returns a list containing path related properties of each s3 object in event
def parse_event(event):
    logging.info(f'Event received: {str(event)}')

    s3_objects = []
    for record in event.get('Records', {}):
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        if len(object_key.split("/")) > 1:
            s3_objects.append({
                'object_path': f"s3://{bucket_name}/{object_key}",
                'object_bucket': bucket_name,
                'object_key': object_key,
                'target_table': f'tbl_{object_key.split("/")[1]}'
            })

    return s3_objects


def get_source_file_path(s3_object):
    if _is_cloud_execution_mode():
        return s3_object['object_path']
    elif _is_local_execution_mode():
        return LOCAL_CSV_FILE_PATH


# gets custom metadata of a single s3 object from s3
def get_s3_object_metadata(s3_object):
    metadata = {}
    if _is_cloud_execution_mode():
        logging.info('Retrieving object metadata.')
        s3 = boto3.client('s3')
        response = s3.head_object(Bucket=s3_object['object_bucket'],
                                  Key=s3_object['object_key'])
        metadata = response.get('Metadata')
        logging.info('Metadata: {}'.format(metadata))

    return metadata


# parses s3 object metadata to identify if table must be partitioned
def get_partition_cols(source_file_path, s3_object_meta):
    if _is_cloud_execution_mode():
        logging.info('Identifying partition columns.')
        partition_cols = s3_object_meta.get('partition-cols')

        if partition_cols:

            try:
                partition_cols = literal_eval(partition_cols)
            except Exception:
                pass

            if isinstance(partition_cols, list):
                partition_cols = [
                    _normalize_name(col) for col in partition_cols
                ]
            elif isinstance(partition_cols, str):
                partition_cols = [_normalize_name(partition_cols)]
            else:
                logging.error(
                    f'Invalid partition-cols metadata for object {source_file_path}.'
                )
                publish_error_to_sns(
                    source_file_path,
                    '\n\nError:\nInvalid partition-cols metadata.')
                raise ValueError(
                    f'Invalid partition-cols metadata for object {source_file_path}. Please specify either a list or a string.'
                )

        logging.info('Partition Columns: {}'.format(partition_cols))
        return partition_cols


# reads csv file from s3 or from local computer
def read_csv(source_file_path, event, s3_object_meta):
    if _is_cloud_execution_mode():
        return _read_csv_cloud(source_file_path, s3_object_meta)
    elif _is_local_execution_mode():
        return _read_csv_local(event)


def _read_csv_cloud(s3_source_path, s3_object_meta):
    logging.info('Extracting csv file from s3.')
    separator = s3_object_meta.get('separator', ',')
    decimal_char = s3_object_meta.get('decimal-char', '.')
    encoding = s3_object_meta.get('file-encoding', 'utf-8')
    try:
        df = pd.read_csv(s3_source_path,
                         sep=separator,
                         decimal=decimal_char,
                         encoding=encoding)
    except Exception as err:
        logging.error(f'Failed to read csv {s3_source_path} on S3.')
        publish_error_to_sns(s3_source_path, f'\n\nError:\n{err}')
        raise err

    return df


def _read_csv_local(event):
    logging.info('Extracting csv file.')
    try:
        df = pd.read_csv(LOCAL_CSV_FILE_PATH,
                         dtype=event['dtypes'],
                         parse_dates=event.get('parse_dates'),
                         na_filter=False)
        for column in event.get('parse_dates', list()):
            df[column] = df[column].dt.date
    except Exception as err:
        logging.error(f'Failed to read csv {LOCAL_CSV_FILE_PATH}.')
        raise err

    return df


# parses s3 object metadata to identify if columns must be casted and then apply cast.
def cast_df_columns(dataframe, source_file_path, s3_object_meta):
    if _is_cloud_execution_mode():
        logging.info('Casting dataframe columns.')
        cast_schema = s3_object_meta.get('custom-cast')
        if cast_schema:
            try:
                cast_schema = literal_eval(cast_schema)
            except Exception as err:
                logging.error(
                    f'Invalid custom-cast format for object {source_file_path}. Dict-like string is expected.'
                )
                publish_error_to_sns(
                    source_file_path,
                    '\n\nError:\nInvalid custom-cast format. Dict-like string is expected.'
                )
                raise err

            for column_name, data_type in cast_schema.items():
                try:
                    if data_type == 'date':
                        dataframe[column_name] = pd.to_datetime(
                            dataframe[column_name]).dt.date
                    elif data_type == 'datetime':
                        dataframe[column_name] = pd.to_datetime(
                            dataframe[column_name])
                    elif data_type == 'int':
                        dataframe[column_name] = dataframe[column_name].astype(
                            'Int64')
                    elif data_type == 'float':
                        dataframe[column_name] = dataframe[column_name].astype(
                            'float64')
                    elif data_type == 'string':
                        dataframe[column_name] = dataframe[column_name].astype(
                            str)
                    else:
                        logging.error(
                            f'Invalid data type {data_type} for column {column_name}.'
                        )
                        publish_error_to_sns(
                            source_file_path,
                            f'\n\nError:\nInvalid data type {data_type} for column {column_name}.'
                        )
                        raise ValueError(
                            f'Unable to cast column {column_name}. Expected either int, float, date, datetime or string and received {data_type}.'
                        )
                except KeyError:
                    logging.warn(
                        f'Skipping cast of column {column_name} to {data_type} as the column does not exists on csv file.'
                    )
                except Exception as err:
                    logging.error(
                        f'Could not cast column {column_name} to {data_type}.')
                    publish_error_to_sns(source_file_path,
                                         f'\n\nError:\n{err}')
                    raise err

    return dataframe


# Applies upper to string columns
def str_columns_to_upper(dataframe, s3_object_meta):
    output_str_upper = s3_object_meta.get('output-str-upper', 'true').lower()
    if output_str_upper == 'true':
        logging.info('Applying upper to string columns.')
        # excludes numerical columns
        df_objects = dataframe.select_dtypes(include='object')
        for column in df_objects.columns:
            dataframe.loc[:, column] = _df_column_to_upper(df_objects[column])
    return dataframe


def _df_column_to_upper(df_column):
    try:
        df_column = df_column.str.upper()
    except Exception:
        # ignores pandas type object that is not string
        pass

    return df_column


# adds metadata to the dataframe regarding the load process.
def add_etl_metadata_to_df(dataframe, source_file_path):
    logging.info('Adding ETL metadata.')
    dataframe['dl_creation_date'] = datetime.today().date()
    dataframe['dl_source_file'] = source_file_path
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
    logging.info('Normalizing column names.')
    columns = list(dataframe.columns)
    dataframe.columns = [_normalize_name(column) for column in columns]
    return dataframe


# Removes NaN values
def replace_nan_values(dataframe):
    logging.info('Replacing NaN values to None.')
    # excludes numerical columns
    df_objects = dataframe.select_dtypes(include='object')
    df_objects = df_objects.replace({np.nan: None})
    dataframe[df_objects.columns] = df_objects
    return dataframe


def build_sns_topic_arn(context):
    if _is_cloud_execution_mode():
        global SNS_TOPIC_ARN
        lambda_arn_splitted = context.invoked_function_arn.split(":")
        aws_region = lambda_arn_splitted[3]
        aws_account_id = lambda_arn_splitted[4]
        SNS_TOPIC_ARN = f'arn:aws:sns:{aws_region}:{aws_account_id}:{SNS_TOPIC_NAME}'


def publish_success_to_sns(s3_object):
    if _is_cloud_execution_mode():
        file_path = s3_object['object_path']
        table_name = s3_object['target_table']
        file_name = file_path.rsplit(sep='/', maxsplit=1)[1]
        subject = f'SUCCESS - {file_name} - Csv to parquet succeeded.'
        message = f'The file {file_path} was loaded successfully into S3 Analytics and is accessible via Athena on table {table_name}.'

        logging.info(f'Publishing success to {SNS_TOPIC_ARN}')
        _publish_to_sns(SNS_TOPIC_ARN, subject, message)


def publish_error_to_sns(file_path, exception_message):
    if _is_cloud_execution_mode():
        file_name = file_path.rsplit(sep='/', maxsplit=1)[1]
        subject = f'ERROR - {file_name} - Csv to parquet failed.'
        message = f'The file {file_path} could not be loaded into S3 Analytics. Please check the logs for more details.\n\n {exception_message}'

        logging.info(f'Publishing error to {SNS_TOPIC_ARN}')
        _publish_to_sns(SNS_TOPIC_ARN, subject, message)


def _publish_to_sns(topic_arn, subject, message):
    client = boto3.client('sns')
    try:
        client.publish(TopicArn=topic_arn, Message=message, Subject=subject)
    except Exception as err:
        logging.error(f'Unable to post to topic {topic_arn}.')
        raise err


def save_as_parquet(dataframe,
                    s3_object,
                    s3_object_meta,
                    partition_cols,
                    event,
                    compression='snappy'):
    if _is_cloud_execution_mode():
        _save_to_s3_as_parquet(dataframe=dataframe,
                               table_name=s3_object['target_table'],
                               partition_cols=partition_cols,
                               compression=compression,
                               source_file_path=s3_object['object_path'],
                               s3_object_meta=s3_object_meta)
    elif _is_local_execution_mode():
        _save_to_local_as_parquet(dataframe=dataframe,
                                  output_path=event.get('output_path'),
                                  partition_cols=event.get('partition_cols'),
                                  compression=compression)


# saves dataframe to s3, creates glue table if not exists and updates glue table's partitions
def _save_to_s3_as_parquet(dataframe, table_name, partition_cols, compression,
                           source_file_path, s3_object_meta):
    logging.info('Saving dataframe to s3.')

    dest_path = f's3://{TARGET_S3_BUCKET}/databases/{TARGET_GLUE_DATABASE}/{table_name}/'
    compression = s3_object_meta.get('output-compression', compression)
    output_mode = s3_object_meta.get('output-mode', 'overwrite_partitions')

    try:
        wr.s3.to_parquet(df=dataframe,
                         path=dest_path,
                         compression=compression,
                         dataset=True,
                         partition_cols=partition_cols,
                         mode=output_mode,
                         database=TARGET_GLUE_DATABASE,
                         table=table_name)
    except Exception as err:
        logging.error(f'Failed to save to S3 on {dest_path}.')
        publish_error_to_sns(source_file_path, f'\n\nError:\n{err}')
        raise err
    logging.info(
        f'Successfully saved dataframe to s3 on {dest_path}. You can query the data on Athena using: select * from {TARGET_GLUE_DATABASE}.{table_name} limit 10;'
    )


def _save_to_local_as_parquet(dataframe, output_path, partition_cols,
                              compression):
    logging.info(f'Saving parquet files locally on: {output_path}')
    dataframe.to_parquet(output_path,
                         partition_cols=_normalize_name(partition_cols),
                         compression=compression)
    logging.info('Parquet files saved successfully.')


# run test
if __name__ == '__main__':
    # test event for running locally
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
        }],
        'partition_cols':
        'ObservationDate',
        'output_path':
        os.path.join(os.path.dirname(__file__), '..\\..\\test-data\\output'),
        'dtypes': {
            'ForecastSiteCode': 'Int64',
            'ObservationTime': 'Int64',
            'ObservationDate': 'str',
            'WindDirection': 'Int64',
            'WindSpeed': 'Int64',
            'WindGust': 'Int64',
            'Visibility': 'Int64',
            'ScreenTemperature': 'float64',
            'Pressure': 'Int64',
            'SignificantWeatherCode': 'Int64',
            'SiteName': 'str',
            'Latitude': 'float64',
            'Longitude': 'float64',
            'Region': 'str',
            'Country': 'str',
        },
        'parse_dates': ['ObservationDate']
    }
    handler(event, {})
