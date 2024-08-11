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
            PartitionKey=str(record['user_id'])  # Changed to 'user_id' for partitioning
        )
        logger.info(f"Sent record with user_id {record['user_id']} to {stream_name}")
        print(f"Sent record with user_id {record['user_id']} to {stream_name}")
    except Exception as e:
        logger.error(f"Error sending record to {stream_name}: {str(e)}")
        print(f"Error sending record to {stream_name}: {str(e)}")

def read_csv_file(file_path):
    """Read CSV file and return rows."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.DictReader(file)
            return [row for row in csv_reader]
    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
        print(f"Error reading {file_path}: {str(e)}")
        return []

def main():
    region_name = 'us-east-2'
    browsing_file = 'data/streams.csv'
    stream_name = 'user_song_streams'

    # Initialize Kinesis client
    kinesis_client = boto3.client('kinesis', region_name=region_name)
    
    # Read user event data from CSV
    user_events = read_csv_file(browsing_file)
    
    # Send records to Kinesis stream
    for record in user_events:
        send_record_to_kinesis(kinesis_client, record, stream_name)
        # time.sleep(0.5) 

if __name__ == "__main__":
    main()