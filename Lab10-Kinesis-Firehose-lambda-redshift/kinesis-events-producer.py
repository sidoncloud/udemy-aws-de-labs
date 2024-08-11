import json
import csv
import boto3
import logging
import time

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_record_to_kinesis(kinesis_client, record, stream_name):
    """Send a single record to the Kinesis stream."""
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=str(record['id'])
        )
        logger.info(f"Sent record with id {record['id']} to {stream_name}")
        print(f"Sent record with id {record['id']} to {stream_name}")
    except Exception as e:
        logger.error(f"Error sending record to {stream_name}: {str(e)}")
        print(f"Error sending record to {stream_name}: {str(e)}")

def read_csv_file(file_path):
    """Read CSV file and return rows."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file_content:
            csv_reader = csv.DictReader(file_content)
            return [row for row in csv_reader]
    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
        print(f"Error reading {file_path}: {str(e)}")
        return []

def main():
    region_name = 'us-east-1'
    browsing_file = 'data/events.csv'
    stream_name = 'app_clickstream_events'

    kinesis_client = boto3.client('kinesis', region_name=region_name)
    user_events = read_csv_file(browsing_file)

    for record in user_events:
        send_record_to_kinesis(kinesis_client, record, stream_name)
        time.sleep(1)

if __name__ == "__main__":
    main()
