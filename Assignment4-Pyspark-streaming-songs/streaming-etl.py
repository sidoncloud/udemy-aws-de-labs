import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, from_json, to_timestamp, window,approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
from awsglue.context import GlueContext
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder.appName("MusicDataAnalysis").getOrCreate()
logger.info("Spark session created successfully.")

# Define the schema for songs data
song_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("track_id", StringType(), True),
    StructField("artists", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
    StructField("danceability", DoubleType(), True),
    StructField("energy", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("mode", IntegerType(), True),
    StructField("speechiness", DoubleType(), True),
    StructField("acousticness", DoubleType(), True),
    StructField("instrumentalness", DoubleType(), True),
    StructField("liveness", DoubleType(), True),
    StructField("valence", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", IntegerType(), True),
    StructField("track_genre", StringType(), True)
])

# Load static data from S3
songs_df = spark.read.schema(song_schema).csv("s3://nl-lab-assignments/songs.csv")

# Define schema for streaming data
stream_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("listen_time", StringType(), True)
])

# Read from Kinesis
raw_stream_df = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "streamARN": "arn:aws:kinesis:us-east-2:127489365181:stream/user_song_streams",
        "classification": "json",
        "startingPosition": "latest",
        "inferSchema": "true"
    }
)

# Parse JSON and select relevant fields
stream_df = raw_stream_df.select(from_json(col("$json$data_infer_schema$_temporary$"), stream_schema).alias("parsed")).select("parsed.*")

# Convert listen_time to timestamp and add watermark
data_frame_with_timestamp = stream_df.withColumn(
    "timestamp", 
    to_timestamp(col("listen_time"), "yyyy-MM-dd HH:mm:ss")
).withWatermark("timestamp", "10 minutes")

# Join streams
joined_df = data_frame_with_timestamp.join(songs_df, "track_id")

# Aggregate metrics using window function
metrics_df = joined_df.groupBy(
    window(col("timestamp"), "30 minutes"), "track_id", "artists", "album_name"
).agg(
    count("user_id").alias("total_listens"),
    approx_count_distinct("user_id").alias("unique_users"),
    avg("duration_ms").alias("average_listen_duration")
)

# Write the output to S3 in append mode
query = metrics_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3://nl-lab-assignments/music_metrics/") \
    .option("checkpointLocation", "s3://nl-lab-assignments/checkpoints/music_metrics/") \
    .start()

logger.info("Streaming job started.")
query.awaitTermination()