import redshift_connector
import boto3
import sys
import json
import logging
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the table name from the arguments
args = getResolvedOptions(sys.argv, ["table_name"])
table_name = args["table_name"]

# Define constants
bucket_name = "s3://nl-aws-de-labs"
redshift_iam_arn = "arn:aws:iam::127489365181:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T164456"
secret_name = "dwh-credentials"
region_name = "us-east-1"

def get_redshift_credentials(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error(f"Unable to retrieve secret: {e}")
        sys.exit(1)  # Exit with an error status

    if 'SecretString' not in get_secret_value_response:
        logger.error("Secret does not contain a string.")
        sys.exit(1)  # Exit with an error status

    secret = get_secret_value_response['SecretString']
    credentials = json.loads(secret)
    return credentials

credentials = get_redshift_credentials(secret_name, region_name)
if not credentials:
    logger.error("Failed to fetch Redshift credentials.")
    sys.exit(1)

user_name = credentials['username']
host = credentials['host']
password = credentials['password']
db_name = credentials['dbname']

try:
    conn = redshift_connector.connect(
        host=host,
        database=db_name,
        port=5439,
        user=user_name,
        password=password
    )
    cursor = conn.cursor()

    # Execute the COPY command
    copy_cmd = f"""
        COPY raw_zone.tmp_{table_name} 
        FROM '{bucket_name}/raw_landing_zone/apartment_db/{table_name}/'
        IAM_ROLE '{redshift_iam_arn}'
        CSV
        IGNOREHEADER 1;
    """
    cursor.execute(copy_cmd)
    logger.info("Data ingestion into temporary Redshift table completed successfully.")

    # Prepare and execute the appropriate MERGE command based on table_name
    # Ensure to fill in the correct SQL command as per your table definitions
    if table_name == 'apartment_viewings':
        merge_cmd = """
             MERGE INTO raw_zone.apartment_viewings
            USING raw_zone.tmp_apartment_viewings AS source
            ON raw_zone.apartment_viewings.apartment_id = source.apartment_id AND 
            raw_zone.apartment_viewings.user_id = source.user_id 
            AND raw_zone.apartment_viewings.viewed_at = source.viewed_at
            WHEN MATCHED THEN
                UPDATE SET
                    is_wishlisted = source.is_wishlisted,
                    call_to_action = source.call_to_action
            WHEN NOT MATCHED THEN
                INSERT (user_id, apartment_id, viewed_at, is_wishlisted, call_to_action)
                VALUES (source.user_id, source.apartment_id, source.viewed_at, source.is_wishlisted, source.call_to_action)
        """
    elif table_name == 'apartments':
        merge_cmd = """
            MERGE INTO raw_zone.apartments
            USING raw_zone.tmp_apartments AS source
            ON raw_zone.apartments.id = source.id
            WHEN MATCHED THEN
                UPDATE SET
                    title = source.title,
                    source = source.source,
                    price = source.price,
                    currency = source.currency,
                    listing_created_on = source.listing_created_on,
                    is_active = source.is_active,
                    last_modified_timestamp = sysdate
            WHEN NOT MATCHED THEN
                INSERT (id, title, source, price, currency, listing_created_on, is_active, last_modified_timestamp)
                VALUES (source.id, source.title, source.source, source.price, source.currency, source.listing_created_on, source.is_active, sysdate)
        """
    elif table_name == 'apartment_attributes':
        merge_cmd = """
            MERGE INTO raw_zone.apartment_attributes
            USING raw_zone.tmp_apartment_attributes AS source
            ON raw_zone.apartment_attributes.id = source.id
            WHEN MATCHED THEN
                UPDATE SET
                    category = source.category,
                    body = source.body,
                    amenities = source.amenities,
                    bathrooms = source.bathrooms,
                    bedrooms = source.bedrooms,
                    fee = source.fee,
                    has_photo = source.has_photo,
                    pets_allowed = source.pets_allowed,
                    price_display = source.price_display,
                    price_type = source.price_type,
                    square_feet = source.square_feet,
                    address = source.address,
                    cityname = source.cityname,
                    state = source.state,
                    latitude = source.latitude,
                    longitude = source.longitude
            WHEN NOT MATCHED THEN
                INSERT (id, category, body, amenities, bathrooms, bedrooms, fee, has_photo, pets_allowed, price_display, price_type, square_feet, address, cityname, state, latitude, longitude)
                VALUES (source.id, source.category, source.body, source.amenities, source.bathrooms, source.bedrooms, source.fee, source.has_photo, source.pets_allowed, source.price_display, source.price_type, source.square_feet, source.address, source.cityname, source.state, source.latitude, source.longitude)
        """
    else:
        logger.error("Invalid table name provided.")
        sys.exit(1)

    cursor.execute(merge_cmd)
    logger.info(f"Data merge into {table_name} completed successfully.")

    # Truncate the temporary table after merge
    truncate_cmd = f"TRUNCATE TABLE raw_zone.tmp_{table_name}"
    cursor.execute(truncate_cmd)
    logger.info(f"Temporary table {table_name} truncated.")

    # Commit all changes
    conn.commit()

except Exception as e:
    logger.error("Error during Redshift operations", exc_info=True)
    conn.rollback()
    sys.exit(1)  # Ensure to exit with an error status if there's an error

finally:
    if conn:
        conn.close()