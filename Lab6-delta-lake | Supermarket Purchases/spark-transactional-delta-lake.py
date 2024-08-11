from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta.tables import DeltaTable
import logging
import boto3
import sys
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# args = getResolvedOptions(sys.argv, ["table_name"])
# table_name = args["table_name"]

table_name = "orders"

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Upsert Data Aggregations") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

# Define S3 bucket paths
s3_bucket_path = "s3://nl-deltalake/"
input_path = f"{s3_bucket_path}/raw_zone/{table_name}/"
output_path = f"{s3_bucket_path}/lakehouse-dwh/{table_name}"

# Load data with explicit schema if necessary or based on table_name
if table_name == 'orders':
    primary_keys = ["order_id"]
elif table_name == 'order_items':
    primary_keys = ["order_id", "id"]
else:
    primary_keys = ["product_id"]

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_path)

# Add date column for partitioning if necessary
if table_name in ['orders', 'order_items']:
    df = df.withColumn("order_date", to_date(col("order_timestamp")))

# Log DataFrame schema to check correctness
logger.info(f"DataFrame schema for table {table_name}: {df.dtypes}")

# Check if the Delta table exists
delta_table_exists = DeltaTable.isDeltaTable(spark, output_path)

if delta_table_exists:
    # Load the Delta table and get its schema
    delta_table = DeltaTable.forPath(spark, output_path)
    
    # Log Delta table schema
    logger.info(f"Delta table schema: {delta_table.toDF().dtypes}")
    
    # Perform UPSERT (MERGE)
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
    delta_table.alias("target").merge(
        df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    if table_name in ['orders', 'order_items']:
        df.write.format("delta").partitionBy("order_date").mode("overwrite").save(output_path)
    else:
        df.write.format("delta").mode("overwrite").save(output_path)

# Move files to archived folder after successful execution
s3 = boto3.client('s3')
bucket_name = s3_bucket_path.split('/')[2]
input_prefix = f"raw_zone/{table_name}/"
archive_prefix = f"archived/{table_name}/"

try:
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix).get('Contents', [])
    for obj in objects:
        source_key = obj['Key']
        destination_key = source_key.replace(input_prefix, archive_prefix)
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
        s3.delete_object(Bucket=bucket_name, Key=source_key)
    logger.info("Files moved to archive successfully.")
except Exception as e:
    logger.error(f"Error moving files to archive: {e}")

# Stop the Spark session
spark.stop()