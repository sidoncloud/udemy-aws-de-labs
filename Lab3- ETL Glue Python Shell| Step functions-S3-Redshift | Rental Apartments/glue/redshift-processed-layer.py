import redshift_connector
import sys, logging,json,boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def create_connection(host, port, database, user, password):
    """Establishes a connection to Redshift."""
    try:
        connection = redshift_connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        logger.info("Connection established successfully.")
        return connection
    except Exception as e:
        logger.error("Failed to establish connection: %s", e)
        sys.exit(1)

def execute_query(cursor, query):
    """Executes a single SQL query."""
    try:
        cursor.execute(query)
        cursor.connection.commit()
        logger.info("Query executed successfully.")
    except Exception as e:
        logger.error("Query execution failed: %s", e)
        cursor.connection.rollback()

def get_last_processed_value(cursor):
    """Fetches the maximum viewed_at timestamp from apartment_viewings."""
    try:
        cursor.execute("SELECT MAX(viewed_at) FROM processed_zone.fact_apartment_viewings;")
        result = cursor.fetchone()
        return result[0] if result[0] else "1970-01-01 00:00:00"
    except Exception as e:
        logger.error("Failed to fetch last processed time: %s", e)
        return "1970-01-01 00:00:00"

def merge_dim_apartments(cursor):
    logger.info("Merging data into dim_apartments.")

    stg_query = f"""
                CREATE TEMP TABLE stage_dim_apartments AS
                SELECT
                    a.id AS apartment_id,
                    a.title,
                    attr.category,
                    attr.body,
                    attr.amenities,
                    attr.bedrooms,
                    attr.bathrooms,
                    attr.square_feet,
                    attr.address,
                    attr.cityname,
                    attr.state,
                    attr.latitude,
                    attr.longitude,
                    attr.has_photo,
                    attr.pets_allowed,
                    attr.price_display,
                    attr.price_type
                FROM
                    raw_zone.apartments a
                JOIN
                    raw_zone.apartment_attributes attr ON a.id = attr.id;
                """
    execute_query(cursor,stg_query)


    merge_query = f"""
                MERGE INTO processed_zone.dim_apartments
                USING stage_dim_apartments AS source
                ON processed_zone.dim_apartments.apartment_id = source.apartment_id
                WHEN MATCHED THEN
                    UPDATE SET
                        title = source.title,
                        category = source.category,
                        body = source.body,
                        amenities = source.amenities,
                        bedrooms = source.bedrooms,
                        bathrooms = source.bathrooms,
                        square_feet = source.square_feet,
                        address = source.address,
                        cityname = source.cityname,
                        state = source.state,
                        latitude = source.latitude,
                        longitude = source.longitude,
                        has_photo = source.has_photo,
                        pets_allowed = source.pets_allowed,
                        price_display = source.price_display,
                        price_type = source.price_type
                WHEN NOT MATCHED THEN
                    INSERT (
                        apartment_id, title, category, body, amenities, bedrooms, bathrooms, square_feet, address, cityname, state, latitude, longitude, has_photo, pets_allowed, price_display, price_type
                    )
                    VALUES (
                        source.apartment_id, source.title, source.category, source.body, source.amenities, source.bedrooms, source.bathrooms, source.square_feet, source.address, source.cityname, source.state, source.latitude, source.longitude, source.has_photo, source.pets_allowed, source.price_display, source.price_type
                    );
                """
    execute_query(cursor,merge_query)
    
def merge_dim_users(cursor):

    logger.info("Merging data into dim_users.")

    stg_query = f"""
                CREATE TEMP TABLE stage_dim_users AS SELECT DISTINCT user_id FROM raw_zone.apartment_viewings
                """
    merge_query = f"""
                    MERGE INTO processed_zone.dim_users 
                    USING stage_dim_users AS source
                    ON processed_zone.dim_users.user_id = source.user_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            user_id = source.user_id
                    WHEN NOT MATCHED THEN
                        INSERT (user_id)
                        VALUES (source.user_id);
                    """
    execute_query(cursor,stg_query)
    execute_query(cursor,merge_query)

def merge_fact_apartment_viewings(cursor, last_processed_value):
    
    logger.info("Performing incremental merge for fact_apartment_viewings.")
    
    stg_query = f"""
        CREATE TEMP TABLE stage_fact_apartment_viewings AS
        SELECT
            v.user_id,
            v.apartment_id,
            v.viewed_at,
            (v.is_wishlisted = 'Y') AS is_wishlisted,
            v.call_to_action,
            a.price,
            attr.fee,
            a.currency
        FROM
            raw_zone.apartment_viewings v
        JOIN
            raw_zone.apartments a ON v.apartment_id = a.id
        JOIN
            raw_zone.apartment_attributes attr ON v.apartment_id = attr.id
        where v.viewed_at >='{last_processed_value}'
    """

    merge_query = f"""
            MERGE INTO processed_zone.fact_apartment_viewings
            USING stage_fact_apartment_viewings AS source
            ON processed_zone.fact_apartment_viewings.apartment_id = source.apartment_id AND processed_zone.fact_apartment_viewings.viewed_at = source.viewed_at
            WHEN MATCHED THEN
                UPDATE SET
                    user_id = source.user_id,
                    is_wishlisted = source.is_wishlisted,
                    call_to_action = source.call_to_action,
                    price = source.price,
                    fee = source.fee,
                    currency = source.currency
            WHEN NOT MATCHED THEN
                INSERT (
                    apartment_id, user_id, viewed_at, is_wishlisted, call_to_action, price, fee, currency
                )
                VALUES (
                    source.apartment_id, source.user_id, source.viewed_at, source.is_wishlisted, source.call_to_action, source.price, source.fee, source.currency
                )"""
    
    execute_query(cursor, stg_query)
    execute_query(cursor,merge_query)

def main():

    secret_name = "dwh-credentials"
    region_name = "us-east-1"
    credentials = get_redshift_credentials(secret_name, region_name)

    user_name = credentials['username']
    host = credentials['host']
    password = credentials['password']
    db_name = credentials['dbname']
    
    try:
        conn = create_connection(
            host=host,
            port=5439,
            database=db_name,
            user=user_name,
            password=password
        )
        conn.autocommit = False
        cursor = conn.cursor()

        last_processed_value = get_last_processed_value(cursor)
        
        # Full merge for dimension tables
        merge_dim_apartments(cursor)
        merge_dim_users(cursor)
        
        # Incremental upsert for fact table
        merge_fact_apartment_viewings(cursor, last_processed_value)
        
    except Exception as e:
        conn.rollback()
        logger.error("An error occurred during processing: %s", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()