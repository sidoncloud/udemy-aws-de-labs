import boto3
import csv
from io import StringIO
from decimal import Decimal
import logging

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize a boto3 client
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('track_level_reports')

# S3 bucket and file path
bucket_name = 'nl-aws-de-labs'
file_path = 'spotify_data/output/song_kpis/'

# Get the latest file from the output folder
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_path)
all_files = response['Contents']
latest_file = max(all_files, key=lambda x: x['LastModified'])['Key']

# Download the latest CSV file content
csv_obj = s3_client.get_object(Bucket=bucket_name, Key=latest_file)
body = csv_obj['Body'].read().decode('utf-8')
csv_data = StringIO(body)
csv_reader = csv.DictReader(csv_data)

# Function to upsert into DynamoDB
def upsert_item(row):
    try:
        # Attempt to parse and insert/update data in DynamoDB
        track_id = row['track_id']
        report_date = row['report_date']
        total_listens = int(row['total_listens'])
        unique_users = int(row['unique_users'])
        total_listening_time = Decimal(str(row['total_listening_time']))
        avg_listening_time_per_user = Decimal(str(row['avg_listening_time_per_user']))

        table.update_item(
            Key={
                'track_id': track_id,
                'report_date': report_date
            },
            UpdateExpression='SET total_listens = :tl, unique_users = :uu, total_listening_time = :tlt, avg_listening_time_per_user = :alt',
            ExpressionAttributeValues={
                ':tl': total_listens,
                ':uu': unique_users,
                ':tlt': total_listening_time,
                ':alt': avg_listening_time_per_user
            },
            ReturnValues="UPDATED_NEW"
        )
        logging.info(f"Successfully processed record for track_id={track_id}, report_date={report_date}")
    except ValueError as e:
        logging.error(f"Error processing row {row}: {e}")

# Process each row in the CSV file
for row in csv_reader:
    upsert_item(row)

# Close the StringIO object
csv_data.close()