import json
import csv
import boto3
import logging
from io import StringIO
import os

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Specify AWS region
aws_region = "us-east-1"

def process_csv_to_kinesis(file_name):
    
    kinesis_client = boto3.client('kinesis', region_name=aws_region)

    logger.info(f"Processing file: {file_name}")

    # Get the file from current working directory
    try:
        file_path = os.path.join(os.getcwd(), file_name)
        with open(file_path, 'r') as file:
            file_content = file.read()
    except Exception as e:
        logger.error(f"Error reading file {file_name} from current directory: {e}")
        raise e

    # Read the file content using csv.DictReader
    csv_data = StringIO(file_content)
    csv_reader = csv.DictReader(csv_data)

    counter = 0
    batch_size = 100
    for row in csv_reader:
        try:
            response = kinesis_client.put_record(
                StreamName="mobile_coverage_logs",
                Data=json.dumps(row),
                PartitionKey=str(hash(row['hour']))
            )
            counter += 1
            # Check response status
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                logger.error('Error sending message to Kinesis:', response)

            # Log after every batch_size records
            if counter % batch_size == 0:
                logger.info(f"Processed {counter} records so far...")

        except Exception as e:
            logger.error(f"Error processing record {row}: {e}")

    logger.info(f"Finished processing. Total records sent: {counter}")
    return f"Processed {counter} records from {file_name}."

if __name__ == "__main__":
    
    file_name = "data/mobile-logs.csv"
    try:
        result = process_csv_to_kinesis(file_name)
        print(result)
    except Exception as e:
        logger.error(f"Error processing file: {e}")