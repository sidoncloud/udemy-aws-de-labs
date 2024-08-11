import boto3
import json
from datetime import datetime

def scan_dynamodb_table(table_name):
    """Scan the DynamoDB table and return all items."""
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.scan()
    data = response['Items']
    
    # Continue scanning if more data is available
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return data

def process_data(data):
    """Process data to calculate aggregations."""
    daily_totals = {}
    
    for item in data:
        fare_amount = float(item['fare_amount']) if 'fare_amount' in item else 0
        date_key = item['pickup_datetime'][:10]  # Extract date from datetime
        
        if date_key not in daily_totals:
            daily_totals[date_key] = {
                'total_fare': fare_amount,
                'count_trips': 1,
                'max_fare': fare_amount,
                'min_fare': fare_amount
            }
        else:
            daily_totals[date_key]['total_fare'] += fare_amount
            daily_totals[date_key]['count_trips'] += 1
            daily_totals[date_key]['max_fare'] = max(fare_amount, daily_totals[date_key]['max_fare'])
            daily_totals[date_key]['min_fare'] = min(fare_amount, daily_totals[date_key]['min_fare'])

    # Calculate average fare per day
    for date_key, totals in daily_totals.items():
        totals['average_fare'] = totals['total_fare'] / totals['count_trips']

    return daily_totals

def save_results_to_s3(bucket, key, data):
    """Save the results to an S3 bucket."""
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, key)
    obj.put(Body=json.dumps(data, indent=4))

def main():
    table_name = 'trip_details'
    s3_bucket = 'nl-aws-de-labs'
    s3_key = f"completed_trips/aggregated_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

    # Scan DynamoDB table for data
    data = scan_dynamodb_table(table_name)
    
    # Process data to get aggregates
    results = process_data(data)
    
    # Save results to S3
    save_results_to_s3(s3_bucket, s3_key, results)
    print(f"Results saved to {s3_bucket}/{s3_key}")

if __name__ == "__main__":
    main()
