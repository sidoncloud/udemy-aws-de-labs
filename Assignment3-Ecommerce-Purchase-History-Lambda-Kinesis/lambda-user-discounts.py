import json
import boto3
import csv
import io
import base64
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')

BUCKET_NAME = 'nl-aws-de-labs'
PURCHASE_HISTORY_FILE = 'purchase_history.csv'
DISCOUNT_STREAM = 'user_cart_discounts'

def load_purchase_history():
    try:
        logger.info(f"Loading purchase history from s3://{BUCKET_NAME}/{PURCHASE_HISTORY_FILE}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=PURCHASE_HISTORY_FILE)
        content = response['Body'].read().decode('utf-8')
        purchase_history = {}
        csv_reader = csv.DictReader(io.StringIO(content))
        for row in csv_reader:
            user_id = row['user_id']
            purchase_history[user_id] = {
                'total_items_purchased': int(row['total_items_purchased']),
                'total_num_orders': int(row['total_num_orders']),
                'total_amount_spent': float(row['total_amount_spent'])
            }
        logger.info("Purchase history loaded successfully.")
        return purchase_history
    except Exception as e:
        logger.error(f"Error loading purchase history: {str(e)}")
        print(f"Error loading purchase history: {str(e)}")
        raise

def lambda_handler(event, context):

    logger.info("Lambda function started.")
    try:
        purchase_history = load_purchase_history()
        
        for record in event['Records']:
            # Decode the base64 encoded data
            payload = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))
            logger.info(f"Processing record for user_id: {payload.get('user_id')}")
            
            user_id = payload['user_id']

            if user_id in purchase_history:  
                
                log_stmt = f"""{user_id} has purchase history"""
                print(log_stmt)
                
                user_data = purchase_history[user_id]

                if int(user_data['total_amount_spent']) > 100:
                    discount_data = {
                        'user_id': user_id,
                        'discount_percentage': 30
                    }
                    try: 
                        kinesis_client.put_record(
                            StreamName=DISCOUNT_STREAM,
                            Data=json.dumps(discount_data),
                            PartitionKey=str(user_id)
                        )
                    except Exception as e:
                        logger.info(str(e))
                    print(f"Sent discount data to {DISCOUNT_STREAM} for user_id: {user_id}")
            else:
                log_msg = f"""{user_id} does not have any purchase history"""
                print(log_msg)

        logger.info("Lambda function completed successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps('Processing complete')
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}")
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }