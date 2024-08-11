import pandas as pd
import awswrangler as wr
import boto3
from decimal import Decimal, Inexact, Rounded
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Define S3 paths for the data
base_path = "s3://nl-aws-de-labs/ecommerce-data/"
orders_path = f"{base_path}orders/"
order_items_path = f"{base_path}order_items/"
products_path = f"{base_path}products/"

# Define S3 paths for archived data
orders_archive_path = f"{base_path}archived/"
order_items_archive_path = f"{base_path}archived/"
products_archive_path = f"{base_path}archived/"

try:
    # Reading datasets
    logger.info("Reading datasets from S3")
    orders = wr.s3.read_csv(path=orders_path)
    order_items = wr.s3.read_csv(path=order_items_path)
    products = wr.s3.read_csv(path=products_path)
except Exception as e:
    logger.error(f"Error reading datasets: {e}")
    raise

try:
    # Join datasets with explicit renaming to avoid column confusion
    logger.info("Joining datasets")
    df = orders.merge(order_items, on="order_id", how='inner', suffixes=('_order', '_item'))
    df = df.merge(products, left_on="product_id", right_on="id", how='inner', suffixes=('', '_product'))

    # Resolve any column name conflicts
    df.rename(columns={'created_at_order': 'order_date', 'status_order': 'order_status'}, inplace=True)

    # Convert 'order_date' to datetime and adjust for UTC
    df['order_date'] = pd.to_datetime(df['order_date'].str.replace(' UTC', ''), format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # Group by date and category for aggregations
    grouped = df.groupby([df['order_date'].dt.date, 'category'])
except Exception as e:
    logger.error(f"Error processing data: {e}")
    raise

def safe_convert_decimal(val):
    """ Convert a value to Decimal safely """
    try:
        return Decimal(str(val))
    except (Inexact, Rounded):
        return Decimal(str(round(val, 2)))

def calculate_kpis(group):
    total_revenue = (group['sale_price'] * group['num_of_item']).sum()
    avg_order_value = group['sale_price'].mean()
    avg_return_rate = group[group['order_status'] == 'Returned']['order_id'].nunique() / group['order_id'].nunique()
    return pd.Series({
        'daily_revenue': safe_convert_decimal(total_revenue),
        'avg_order_value': safe_convert_decimal(avg_order_value),
        'avg_return_rate': safe_convert_decimal(avg_return_rate)
    })

try:
    df_category_wise_summary = grouped.apply(calculate_kpis).reset_index()
except Exception as e:
    logger.error(f"Error calculating category-wise KPIs: {e}")
    raise

def calculate_order_kpis(group):
    total_orders = group['order_id'].nunique()
    total_revenue = (group['sale_price'] * group['num_of_item']).sum()
    total_items_sold = group['num_of_item'].sum()
    return_rate = group[group['order_status'] == 'Returned']['order_id'].nunique() / total_orders
    unique_customers = group['user_id_order'].nunique()
    
    return pd.Series({
        'total_orders': total_orders,
        'total_revenue': safe_convert_decimal(total_revenue),
        'total_items_sold': total_items_sold,
        'return_rate': safe_convert_decimal(return_rate),
        'unique_customers': unique_customers
    })

try:
    df_daily_order_summary = df.groupby(df['order_date'].dt.date).apply(calculate_order_kpis).reset_index()
except Exception as e:
    logger.error(f"Error calculating daily order KPIs: {e}")
    raise

dynamodb = boto3.resource('dynamodb')
kpi_table = dynamodb.Table('category_wise_summary')
order_kpi_table = dynamodb.Table('daily_order_summary')

# Function to upsert data into DynamoDB
def upsert_dynamodb(table, item):
    try:
        table.put_item(Item=item)
    except Exception as e:
        logger.error(f"Error upserting data into DynamoDB: {e}")
        raise

# Insert category-level metrics into DynamoDB
try:
    logger.info("Inserting category-wise KPI data into DynamoDB")
    for index, row in df_category_wise_summary.iterrows():
        kpi_data = {
            'category': row['category'],
            'order_date': str(row['order_date']),
            'daily_revenue': safe_convert_decimal(row['daily_revenue']),
            'avg_order_value': safe_convert_decimal(row['avg_order_value']),
            'avg_return_rate': safe_convert_decimal(row['avg_return_rate'])
        }
        upsert_dynamodb(kpi_table, kpi_data)
except Exception as e:
    logger.error(f"Error inserting category-wise KPI data: {e}")
    raise

# Insert order-level metrics into DynamoDB
try:
    logger.info("Inserting daily order KPI data into DynamoDB")
    for index, row in df_daily_order_summary.iterrows():
        order_kpi_data = {
            'order_date': str(row['order_date']),
            'total_orders': row['total_orders'],
            'total_revenue': safe_convert_decimal(row['total_revenue']),
            'total_items_sold': row['total_items_sold'],
            'return_rate': safe_convert_decimal(row['return_rate']),
            'unique_customers': row['unique_customers']
        }
        upsert_dynamodb(order_kpi_table, order_kpi_data)
except Exception as e:
    logger.error(f"Error inserting daily order KPI data: {e}")
    raise

logger.info("KPIs calculated and saved successfully to DynamoDB tables category_wise_summary and daily_order_summary")

# Function to move files in S3
s3_client = boto3.client('s3')

def move_s3_files(source_path, archive_path):
    """Move files from source to archive folder in S3."""
    s3_client = boto3.client('s3')
    bucket = 'nl-aws-de-labs'
    source_prefix = source_path.replace("s3://nl-aws-de-labs/", "")
    archive_prefix = archive_path.replace("s3://nl-aws-de-labs/", "")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=source_prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                source_key = obj['Key']
                archive_key = source_key.replace(source_prefix, archive_prefix)
                # Ensure the new key is correctly constructed for the archive path
                if not archive_key.startswith(archive_prefix):
                    archive_key = archive_prefix + source_key.split('/')[-1]

                # Copy the file to the new location
                s3_client.copy_object(
                    CopySource={'Bucket': bucket, 'Key': source_key},
                    Bucket=bucket,
                    Key=archive_key
                )
                # Delete the original file
                s3_client.delete_object(Bucket=bucket, Key=source_key)
        logger.info(f"Moved files from {source_path} to {archive_path} successfully")
    except Exception as e:
        logger.error(f"Error moving files from {source_path} to {archive_path}: {e}")
        raise Exception(f"Error moving files from {source_path} to {archive_path}: {e}")

# Move files to archived folder after DynamoDB update
try:
    move_s3_files(orders_path, orders_archive_path)
    move_s3_files(order_items_path, order_items_archive_path)
    move_s3_files(products_path, products_archive_path)
except Exception as e:
    logger.error(f"Error moving files to archived folders: {e}")
    raise

logger.info("Files moved to archived folders successfully.")