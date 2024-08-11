from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as _sum, avg, approx_count_distinct, expr, rank, desc
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Spotify Advanced KPI Processing") \
    .getOrCreate()

# Define S3 bucket and file paths
bucket_name = 'nl-aws-de-labs'
songs_file_path = f's3a://{bucket_name}/spotify_data/songs/'
users_file_path = f's3a://{bucket_name}/spotify_data/users/'
user_streams_path = f's3a://{bucket_name}/spotify_data/user-streams/'

# Read the CSV files into DataFrames
songs_df = spark.read.csv(songs_file_path, header=True, inferSchema=True)
users_df = spark.read.csv(users_file_path, header=True, inferSchema=True)
user_streams_df = spark.read.csv(user_streams_path + "*", header=True, inferSchema=True)

# Extract the date from listen_time to use as report_date
user_streams_df = user_streams_df.withColumn("report_date", to_date(col("listen_time")))

# Ensure there are no null keys where joins are going to happen
user_streams_df = user_streams_df.filter(user_streams_df["track_id"].isNotNull())
songs_df = songs_df.filter(songs_df["track_id"].isNotNull())

# Calculate KPIs for each song on a daily basis
song_kpis_df = user_streams_df.groupBy("track_id", "report_date").agg(
    count("*").alias("total_listens"),
    approx_count_distinct("user_id").alias("unique_users"),
    _sum(expr("unix_timestamp(listen_time)")).alias("total_listening_time"),
    avg(expr("unix_timestamp(listen_time)")).alias("avg_listening_time_per_user")
)

# Join with songs data to get genre
song_kpis_with_details_df = song_kpis_df.join(songs_df, "track_id")

# Window specification for ranking songs within each genre by total_listens
windowSpec = Window.partitionBy("report_date", "track_genre").orderBy(desc("total_listens"))
ranked_songs_df = song_kpis_with_details_df.withColumn("rank", rank().over(windowSpec))

# Filter for top 3 songs per genre per day
top_songs_per_genre = ranked_songs_df.filter(ranked_songs_df.rank <= 3)

# Find the top 5 genres based on total listens across all songs and all days
genre_window = Window.partitionBy("report_date").orderBy(desc("total_listens"))
top_genres = top_songs_per_genre.withColumn("genre_rank", rank().over(genre_window)) \
               .filter(col("genre_rank") <= 5)

final_df = top_genres.select(
    col("report_date"),
    col("track_id"),
    col("track_name"),
    col("artists"),
    col("track_genre"),
    col("total_listens"),
    col("unique_users"),
    col("total_listening_time"),
    col("avg_listening_time_per_user")
)

# Write the final DataFrame to the same S3 bucket as a CSV file
output_path = f's3a://{bucket_name}/spotify_data/output/song_kpis/'
final_df.write.mode("overwrite").csv(output_path, header=True)
spark.stop()