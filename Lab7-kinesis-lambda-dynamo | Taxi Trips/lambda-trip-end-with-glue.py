import boto3
import base64
import json
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

region_name = 'us-east-1'

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name=region_name)
    trip_details_table = dynamodb.Table('trip_details')
    glue = boto3.client('glue', region_name=region_name)

    record_count = 0
    error_count = 0
    glue_triggered_count = 0

    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data_item = json.loads(payload)

            # Check if trip_id exists in DynamoDB
            response = trip_details_table.get_item(Key={'trip_id': data_item['trip_id']})
            if 'Item' not in response:
                logger.info(f"Skipping trip_id {data_item['trip_id']} as it does not exist in trip_details.")
                continue

            # Convert numerical values to Decimal
            for field in ['fare_amount', 'tip_amount', 'total_amount']:
                if field in data_item:
                    data_item[field] = Decimal(str(data_item[field]))

            # Upsert the data into DynamoDB
            update_expression = "SET " + ", ".join([f"{k} = :{k}" for k in data_item if k != 'trip_id'])
            expression_attribute_values = {f":{k}": v for k, v in data_item.items() if k != 'trip_id'}
            update_response = trip_details_table.update_item(
                Key={'trip_id': data_item['trip_id']},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW"
            )

            if update_response['Attributes']:
                # Trigger AWS Glue job if update successful
                glue_response = glue.start_job_run(JobName='process_completed_trips')
                logger.info(f"Triggered Glue job: {glue_response['JobRunId']} for trip_id {data_item['trip_id']}")
                glue_triggered_count += 1

            record_count += 1
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            error_count += 1

    logger.info(f'Updated {record_count} trips with {error_count} errors. Triggered Glue job {glue_triggered_count} times.')
    return {
        'statusCode': 200,
        'body': json.dumps(f'Updated {record_count} trips with {error_count} errors. Triggered Glue job {glue_triggered_count} times.')
    }