import json
import csv
import boto3
import logging
import time
import random

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_batch_to_kinesis(kinesis_client, batch, stream_name):
    """Send a batch of records to the Kinesis stream."""
    if not batch:
        logger.info(f"No records to send to {stream_name}.")
        return
    try:
        records = [{'Data': json.dumps(record), 'PartitionKey': record['trip_id']} for record in batch]
        response = kinesis_client.put_records(Records=records, StreamName=stream_name)
        successful_records = len(batch) - response['FailedRecordCount']
        logger.info(f"Sent {successful_records}/{len(batch)} records to {stream_name}")
        if response['FailedRecordCount'] > 0:
            logger.error(f"Failed to send {response['FailedRecordCount']} records.")
        return [record['trip_id'] for record in batch if record['trip_id']]  # Return trip_ids of successful records
    except Exception as e:
        logger.error(f"Error sending records to {stream_name}: {str(e)}")
        return []

def read_csv_file(file_path):
    """Read CSV file and return rows."""
    try:
        with open(file_path, mode='r', encoding='utf-8') as file_content:
            csv_reader = csv.DictReader(file_content)
            return [row for row in csv_reader]
    except Exception as e:
        logger.error(f"Error reading {file_path}: {str(e)}")
        return []

def main():
    region_name = 'us-east-1'
    trip_start_file = 'data/trip_start.csv'
    trip_end_file = 'data/trip_end.csv'
    start_stream_name = 'trip_start_stream'
    end_stream_name = 'trip_end_stream'
    batch_size = 30

    kinesis_client = boto3.client('kinesis', region_name=region_name)

    start_trips = read_csv_file(trip_start_file)
    end_trips = read_csv_file(trip_end_file)

    for i in range(0, len(start_trips), batch_size):
        batch = start_trips[i:i + batch_size]
        sent_trip_ids = send_batch_to_kinesis(kinesis_client, batch, start_stream_name)

        # Delay between sending start and end trips
        time.sleep(random.randint(1,2))

        if sent_trip_ids:
            filtered_end_trips = [trip for trip in end_trips if trip['trip_id'] in sent_trip_ids]
            send_batch_to_kinesis(kinesis_client, filtered_end_trips, end_stream_name)

        logger.info("Completed a batch processing cycle. Waiting before the next batch.")
        time.sleep(5)

if __name__ == "__main__":
    main()