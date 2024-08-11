import boto3, sys, csv, pymysql, io, json, logging
from datetime import date
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

args = getResolvedOptions(sys.argv, ["table_name","delta_value"])
table_name = args["table_name"]
delta_value = args["delta_value"]

def get_rds_credentials(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Unable to retrieve secret: {e}")
        return None

    if 'SecretString' not in get_secret_value_response:
        logger.error("Secret does not contain a string.")
        return None

    secret = get_secret_value_response['SecretString']
    credentials = json.loads(secret)
    return credentials

credentials = get_rds_credentials("flights_db", "us-east-2")

# DB Credentials
user_name = credentials['username']
host = credentials['host']
password = credentials['password']
db_name = "flights_db"

# S3 bucket Path
s3_bucket = "nl-lab-assignments"
s3_key = f'raw_landing_zone/{db_name}/{table_name}/data.csv'

def main():
    try:
        connection = pymysql.connect(
            host=host,
            user=user_name,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )

        if table_name=='flights':
            sql = f"SELECT * FROM {table_name} where date(created_at)='{delta_value}'"
            
        else:
            sql = f"SELECT * FROM {table_name}"

        with connection.cursor() as cursor:
            
            cursor.execute(sql)
            result = cursor.fetchall()

        csv_data = convert_to_csv(result)

        s3 = boto3.client('s3')
        s3.put_object(Body=csv_data, Bucket=s3_bucket, Key=s3_key)
        logger.info('Data extracted and written to S3 successfully')

    except Exception as e:
        logger.error(f'Error: {str(e)}')
        sys.exit(1)

    finally:
        connection.close()

def convert_to_csv(data):
    if not data:
        return ""
    csv_file = io.StringIO()
    fieldnames = data[0].keys()
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    return csv_file.getvalue()

if __name__ == '__main__':
    main()