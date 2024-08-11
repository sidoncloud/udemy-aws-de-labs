from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, count, sum, avg, max, min

spark = SparkSession.builder.appName("TransactionUserAnalysis").getOrCreate()

# Load datasets
transactions = spark.read.csv("s3://nl-aws-de-labs/rental_vehicles/rental_transactions.csv", header=True, inferSchema=True)
users = spark.read.csv("s3://nl-aws-de-labs/rental_vehicles/users.csv", header=True, inferSchema=True)

# Convert timestamps
transactions = transactions.withColumn("rental_start_time", to_timestamp(col("rental_start_time")))
transactions = transactions.withColumn("rental_end_time", to_timestamp(col("rental_end_time")))
transactions = transactions.withColumn("duration_hours", (col("rental_end_time").cast("long") - col("rental_start_time").cast("long")) / 3600)

# Join transactions with users
transaction_details = transactions.join(users, "user_id")

# Transaction-level metrics
transaction_metrics = transaction_details.groupBy(window(col("rental_start_time"), "1 day").alias("rental_date")) \
    .agg(
        count("*").alias("total_transactions"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("average_transaction_value"),
        max("duration_hours").alias("max_rental_duration"),
        min("duration_hours").alias("min_rental_duration"),
        avg("duration_hours").alias("avg_rental_duration")
    )

# User engagement metrics
user_metrics = transaction_details.groupBy("user_id") \
    .agg(
        count("*").alias("total_user_transactions"),
        sum("total_amount").alias("user_total_revenue"),
        avg("total_amount").alias("average_user_spending"),
        sum("duration_hours").alias("total_user_rental_hours"),
        max("total_amount").alias("max_user_spending"),
        min("total_amount").alias("min_user_spending")
    )

# transaction_metrics.show(10)
# user_metrics.show(10)

# Write results to S3
transaction_metrics.write.parquet("s3://nl-aws-de-labs/rental_vehicles/output/transaction_metrics", mode="overwrite")
user_metrics.write.parquet("s3://nl-aws-de-labs/rental_vehicles/output/user_metrics", mode="overwrite")

spark.stop()