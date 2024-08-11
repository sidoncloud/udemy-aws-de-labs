import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import window, col, from_json, avg, to_timestamp, concat, lit, date_format,unix_timestamp
from awsglue.context import GlueContext
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

try:
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = SparkSession.builder.appName("KinesisDataAnalysis").getOrCreate()
    logger.info("Spark session created successfully.")
    
    # Define the schema for the new dataset
    schema = StructType([
        StructField("hour", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("signal", StringType(), True),
        StructField("network", StringType(), True),
        StructField("operator", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("satellites", StringType(), True),
        StructField("precission", DoubleType(), True),
        StructField("provider", StringType(), True),
        StructField("activity", StringType(), True),
        StructField("postal_code", StringType(), True)
    ])
    
    # Read from Kinesis
    raw_data_frame = glueContext.create_data_frame.from_options(
        connection_type="kinesis",
        connection_options={
            "streamARN": "arn:aws:kinesis:us-east-1:127489365181:stream/mobile_coverage_logs",
            "classification": "json",
            "startingPosition": "latest",
            "inferSchema": "true"
        }
    )
    
    data_frame = raw_data_frame.select(from_json(col("$json$data_infer_schema$_temporary$"), schema).alias("parsed")).select("parsed.*")
    
    data_frame_with_timestamp = data_frame.withColumn(
        "timestamp", 
        unix_timestamp(concat(lit("2023-01-01 "), col("hour")), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
    ).withColumn(
        "partition_hour", 
        date_format(col("timestamp"), "HH")
    )

    # Extract hour for partitioning
    data_frame_with_partition_hour = data_frame_with_timestamp.withColumn(
        "partition_hour", 
        date_format(col("timestamp"), "HH")
    )
    
    data_frame_with_watermark = data_frame_with_partition_hour.withWatermark("timestamp", "10 minutes")
    
    signal_strength_by_operator_df = data_frame_with_watermark.groupBy(
        window(col("timestamp"), "2 minutes"), "postal_code", "operator", "partition_hour"
    ).agg(
        avg("signal").alias("average_signal")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "postal_code",
        "operator",
        "partition_hour",
        "average_signal"
    )
    
    gps_precision_by_provider_df = data_frame_with_watermark.groupBy(
        window(col("timestamp"), "2 minutes"), "provider","partition_hour"
    ).agg(
        avg("precission").alias("average_precision"),
        avg("satellites").alias("average_satellites")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "provider",
        "partition_hour",
        "average_precision",
        "average_satellites"
    )
    
    status_count_df = data_frame_with_watermark.groupBy(
        window(col("timestamp"), "2 minutes"), "postal_code", "status", "partition_hour"
    ).count().select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "postal_code",
        "status",
        "partition_hour",
        col("count").alias("status_count")
    )
    
    s3_path = "s3://nl-streaming-output/aggregations/"
    s3_path_checkpoint = "s3://nl-streaming-output/checkpoints/"
    
    signal_strength_by_operator = signal_strength_by_operator_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", s3_path_checkpoint + "signal_strength_by_operator") \
        .option("path", s3_path + "signal_strength_by_operator/") \
        .partitionBy("partition_hour", "postal_code") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    gps_precision_by_provider = gps_precision_by_provider_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", s3_path_checkpoint + "gps_precision_by_provider") \
        .option("path", s3_path + "gps_precision_by_provider/") \
        .partitionBy("partition_hour", "provider") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    status_count = status_count_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", s3_path_checkpoint + "status_count") \
        .option("path", s3_path + "status_count/") \
        .partitionBy("partition_hour","postal_code") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    logger.info("Starting the streaming jobs.")
    signal_strength_by_operator.awaitTermination()
    gps_precision_by_provider.awaitTermination()
    status_count.awaitTermination()
    
except Exception as e:
    logger.error("An error occurred: ", exc_info=True)