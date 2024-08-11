import pymysql
import boto3,io
import sys  # Import sys to use sys.exit()
import csv  # Import csv to write files in CSV format
from datetime import date
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["table_name","load_type"])

table_name = args["table_name"]
load_type = args["load_type"]

incr_column = ""
if table_name=='apartment_viewings':
    incr_column = "viewed_at"
elif table_name=='apartments':
    incr_column = "listing_created_on"

# table_name = "apartment_viewings"
# load_type = "incremental"

# Database configuration
db_endpoint = "apartment-rentals.c1yklxlrd3gk.us-east-1.rds.amazonaws.com"
db_username = "admin"
db_password = "Etl123$$"
db_name = "rental_apartments"
s3_bucket = "nl-aws-de-labs"

if '--date_filter' in sys.argv:
    optional_args = getResolvedOptions(sys.argv, ["date_filter"])
    date_filter = optional_args['date_filter']
else: 
    date_filter = date.today()

if load_type=="incremental":
    s3_key = f'raw_landing_zone/apartment_db/{table_name}/{date_filter}/data.csv'
else:
    s3_key = f'raw_landing_zone/apartment_db/{table_name}/data.csv'

def main():
    try:
        # Connect to the RDS database
        connection = pymysql.connect(
            host=db_endpoint,
            user=db_username,
            password=db_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )

        # Execute SQL query to fetch data
        with connection.cursor() as cursor:
            if load_type=='incremental':
                sql = f"SELECT * FROM {table_name} where date({incr_column})='{date_filter}'"
            else:
                sql = f"SELECT * FROM {table_name}"
            cursor.execute(sql)
            result = cursor.fetchall()
        
        # Generate CSV from data
        csv_data = convert_to_csv(result)

        # Write data to S3
        s3 = boto3.client('s3')
        s3.put_object(Body=csv_data, Bucket=s3_bucket, Key=s3_key)

        print('Data extracted and written to S3 successfully')

    except Exception as e:
        print(f'Error: {str(e)}')
        sys.exit(1)  # Exit with status 1 to indicate failure

    finally:
        if 'connection' in locals() and connection:
            connection.close()

def convert_to_csv(data):
    """ Convert the list of dictionaries to CSV format. """
    if not data:
        return ""
    # Create a CSV string from dictionary data
    csv_file = io.StringIO()
    fieldnames = data[0].keys()  # Assumes all dictionaries have the same keys
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    return csv_file.getvalue()

if __name__ == '__main__':
    main()