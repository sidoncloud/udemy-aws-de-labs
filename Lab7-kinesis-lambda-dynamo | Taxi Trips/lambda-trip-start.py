import boto3
import base64
import json
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Specify the AWS region
region_name = 'us-east-1'

def lambda_handler(event, context):
    # Initialize DynamoDB resource and specify the table
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    trip_details_table = dynamodb.Table('trip_details')

    record_count = 0
    error_count = 0

    for record in event['Records']:
        try:
            # Decode the data from Kinesis
            payload = base64.b64decode(record['kinesis']['data'])
            data_item = json.loads(payload)

            # Convert numerical values to Decimal
            for field in ['estimated_fare_amount']:
                data_item[field] = Decimal(str(data_item[field]))

            # Prepare and insert the data into DynamoDB
            trip_details_table.put_item(Item=data_item)
            record_count += 1
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            error_count += 1

    logger.info(f'Processed {record_count} records with {error_count} errors.')

    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {record_count} records with {error_count} errors.')
    }