from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, min, max, window, udf
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType, TimestampType
import logging

# Import functions from calculate.py
from calculate import calculate_heat_index, calculate_dew_point, calculate_wind_chill

# Set logging level to WARN
logging.getLogger("org").setLevel(logging.WARN)
logging.getLogger("py4j").setLevel(logging.WARN)

# Define schema for weather data
weather_schema = StructType() \
    .add("city", StringType()) \
    .add("country", StringType()) \
    .add("temperature", FloatType()) \
    .add("feels_like", FloatType()) \
    .add("temp_min", FloatType()) \
    .add("temp_max", FloatType()) \
    .add("pressure", IntegerType()) \
    .add("humidity", IntegerType()) \
    .add("visibility", StringType()) \
    .add("wind_speed", FloatType()) \
    .add("wind_deg", IntegerType()) \
    .add("rain_1h", FloatType()) \
    .add("cloud_coverage", IntegerType()) \
    .add("description", StringType()) \
    .add("icon", StringType()) \
    .add("timestamp", TimestampType())

# Initialize Spark session with Kafka settings
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .master("local[*]") \
    .config("spark.kafka.bootstrap.servers", "localhost:29092") \
    .getOrCreate()

# Read data from the "weatherDetails" Kafka topic
weather_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "weatherDetails") \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize and parse JSON messages
weather_data = weather_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), weather_schema).alias("data")) \
    .select("data.*")

# Define UDFs for calculations
@udf(FloatType())
def udf_calculate_heat_index(temperature, humidity):
    return calculate_heat_index(temperature, humidity)

@udf(FloatType())
def udf_calculate_dew_point(temperature, humidity):
    return calculate_dew_point(temperature, humidity)

@udf(FloatType())
def udf_calculate_wind_chill(temperature, wind_speed):
    return calculate_wind_chill(temperature, wind_speed)

# Add calculated columns to the data
weather_data_with_calculations = weather_data \
    .withColumn("heat_index", udf_calculate_heat_index(col("temperature"), col("humidity"))) \
    .withColumn("dew_point", udf_calculate_dew_point(col("temperature"), col("humidity"))) \
    .withColumn("wind_chill", udf_calculate_wind_chill(col("temperature"), col("wind_speed")))

# Function to write metrics data to Kafka ("metricsWeather" topic)
def write_to_metrics_weather(batch_df, batch_id):
    batch_df.selectExpr(
        "CAST(timestamp AS STRING) AS key", 
        "TO_JSON(struct(city, timestamp, heat_index, dew_point, wind_chill)) AS value"
    ).write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("topic", "metricsWeather") \
    .save()

# Write calculated metrics to "metricsWeather" topic
query_metrics = weather_data_with_calculations.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_metrics_weather) \
    .trigger(processingTime='60 minutes') \
    .start()

# Aggregate metrics
aggregated_metrics = weather_data_with_calculations \
    .groupBy(window(col("timestamp"), "60 minutes"), col("city")) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
        avg("humidity").alias("avg_humidity"),
        min("humidity").alias("min_humidity"),
        max("humidity").alias("max_humidity")
    )

# Function to write aggregated data to Kafka ("aggregatedWeather" topic)
def write_to_aggregated_weather(batch_df, batch_id):
    batch_df.selectExpr(
        "CAST(window.start AS STRING) AS key", 
        "TO_JSON(struct(*)) AS value"
    ).write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("topic", "aggregatedWeather") \
    .save()

# Write aggregated data to "aggregatedWeather" topic
query_aggregated = aggregated_metrics.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_aggregated_weather) \
    .trigger(processingTime='60 minutes') \
    .start()

# Wait for all streams to finish
query_metrics.awaitTermination()
query_aggregated.awaitTermination()
