import boto3
import pandas as pd
import logging
import json
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# S3 Bucket and Prefixes
s3_bucket = 'nl-aws-de-labs'

folders = {
    'orders': 'ecommerce-data/new/orders',
    'order_items': 'ecommerce-data/new/order_items',
    'products': 'ecommerce-data/new/products'
}

ready_folders = {
    'orders': 'ecommerce-data/orders/',
    'order_items': 'ecommerce-data/order_items/',
    'products': 'ecommerce-data/products/'
}

def list_files(bucket, prefix):
    """List files in an S3 bucket under a specific prefix."""
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in response_iterator:
        if 'Contents' in page:
            files += [content['Key'] for content in page['Contents'] if content['Key'].endswith('.csv')]
    return files

def read_from_s3(bucket, file_key):
    """Read a file from S3 into a pandas DataFrame."""
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=file_key)
    df = pd.read_csv(obj['Body'])
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)
        df['created_at'] = df['created_at'].dt.tz_localize(None)
    return df

def check_format(df, column_formats, file_key):
    """Check if all required columns exist and their formats are correct."""
    missing_columns = [col for col in column_formats if col not in df.columns]
    if missing_columns:
        missing_columns_str = ", ".join(missing_columns)
        logger.error(f"Missing columns: {missing_columns_str} in {file_key} dataset")
        return False
    for column, expected_type in column_formats.items():
        if column in df.columns:
            if not pd.api.types.is_dtype_equal(df[column].dtype, expected_type):
                logger.error(f"Format check failed for column {column}: expected {expected_type}, found {df[column].dtype} in {file_key} dataset")
                return False
        else:
            logger.error(f"Column {column} is missing from the DataFrame in {file_key} dataset.")
            return False
    return True

def move_file(s3_client, bucket, file_key, ready_prefix):
    """Move a file to the ready folder."""
    copy_source = {'Bucket': bucket, 'Key': file_key}
    new_key = ready_prefix + file_key.split('/')[-1]
    s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key=new_key)
    s3_client.delete_object(Bucket=bucket, Key=file_key)

def main():
    
    task_token = os.getenv('TASK_TOKEN')
    if not task_token:
        logger.error("TASK_TOKEN environment variable is missing")
        return
    
    results = {"orders": 0, "order_items": 0, "products": 0}
    s3_client = boto3.client('s3')

    for folder_key in folders.keys():
        prefix = folders[folder_key]
        files = list_files(s3_bucket, prefix)
        column_formats = {
            'orders': {
                'order_id': 'int64',
                'user_id': 'int64',
                'status': 'object',
                'created_at': 'datetime64[ns]',
                'num_of_item': 'int64'
            },
            'order_items': {
                'id': 'int64',
                'order_id': 'int64',
                'user_id': 'int64',
                'product_id': 'int64',
                'created_at': 'datetime64[ns]'
            },
            'products': {
                'id': 'int64',
                'sku': 'object',
                'cost': 'float64',
                'category': 'object',
                'name': 'object',
                'retail_price': 'float64',
                'department': 'object'
            }
        }

        for file_name in files:
            df = read_from_s3(s3_bucket, file_name)
            format_result = check_format(df, column_formats[folder_key], folder_key)
            if format_result:
                results[folder_key] += 1
                move_file(s3_client, s3_bucket, file_name, ready_folders[folder_key])

    logger.info(f"Results: {json.dumps(results)}")

    stepfunctions_client = boto3.client('stepfunctions')
    response = stepfunctions_client.send_task_success(
        taskToken=task_token,
        output=json.dumps(results)
    )
    logger.info("Task output sent to Step Functions successfully")

if __name__ == '__main__':
    main()