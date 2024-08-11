from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, sum, count, avg, max, min, countDistinct

spark = SparkSession.builder.appName("VehicleLocationPerformance").getOrCreate()

# Load datasets
transactions = spark.read.csv("s3://nl-aws-de-labs/rental_vehicles/rental_transactions.csv", header=True, inferSchema=True)
locations = spark.read.csv("s3://nl-aws-de-labs/rental_vehicles/locations.csv", header=True, inferSchema=True)
vehicles = spark.read.csv("s3://nl-aws-de-labs/rental_vehicles/vehicles.csv", header=True, inferSchema=True)

# Convert timestamps and calculate duration in hours
transactions = transactions.withColumn("rental_start_time", to_timestamp(col("rental_start_time")))
transactions = transactions.withColumn("rental_end_time", to_timestamp(col("rental_end_time")))
transactions = transactions.withColumn("duration_hours", (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600)

# Join transactions with locations and vehicles
location_vehicle_details = transactions.join(locations, transactions["pickup_location"] == locations["location_id"], "inner") \
                                      .join(vehicles, "vehicle_id", "inner")

# Metrics by location
location_performance = location_vehicle_details.groupBy("location_id") \
    .agg(
        sum("total_amount").alias("location_total_revenue"),
        count("*").alias("location_total_transactions"),
        avg("total_amount").alias("average_transaction_amount_at_location"),
        max("total_amount").alias("max_transaction_amount"),
        min("total_amount").alias("min_transaction_amount"),
        countDistinct("vehicle_id").alias("unique_vehicles_used_at_location")
    )

# Metrics by vehicle type
vehicle_performance = location_vehicle_details.groupBy("vehicle_type") \
    .agg(
        sum("total_amount").alias("type_total_revenue"),
        count("*").alias("type_total_transactions"),
        avg("total_amount").alias("average_transaction_amount_by_type"),
        max("total_amount").alias("max_transaction_amount_by_type"),
        min("total_amount").alias("min_transaction_amount_by_type"),
        avg("duration_hours").alias("avg_rental_duration_by_type")
    )

# Output results to console for verification (comment out when running in production)
# location_performance.show(10)
# vehicle_performance.show(10)

# Write results to S3
location_performance.write.parquet("s3://nl-aws-de-labs/rental_vehicles/output/location_performance_metrics", mode="overwrite")
vehicle_performance.write.parquet("s3://nl-aws-de-labs/rental_vehicles/output/vehicle_performance_metrics", mode="overwrite")

spark.stop()
