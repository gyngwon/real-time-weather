from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, min, max, sum, to_date, udf, first
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import logging
import redis
import json

# Import your calculation functions
from calculate import calculate_heat_index, calculate_dew_point, calculate_wind_chill

# Set logging level to WARN
logging.getLogger("org").setLevel(logging.WARN)
logging.getLogger("py4j").setLevel(logging.WARN)

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("feels_like", FloatType(), True),
    StructField("temp_min", FloatType(), True),
    StructField("temp_max", FloatType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("visibility", StringType(), True),
    StructField("wind_speed", FloatType(), True),
    StructField("wind_deg", IntegerType(), True),
    StructField("rain_1h", FloatType(), True),
    StructField("cloud_coverage", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("icon", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaToConsole") \
    .config("spark.driver.host","127.0.0.1") \
    .getOrCreate()

# Set the logging level for the Spark application
spark.sparkContext.setLogLevel("WARN")

# Read stream from Kafka with starting offsets
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the key and value from Kafka to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Parse the JSON value and select the relevant fields
weather_df = df.select(
    from_json(col("value"), schema).alias("weather_data")
).select("weather_data.*")

# Convert the timestamp to date for daily aggregation
weather_df = weather_df.withColumn("date", to_date(col("timestamp")))

# Register UDFs for calculations
@udf(returnType=FloatType())
def udf_heat_index(temp, humidity):
    return calculate_heat_index(temp, humidity)

@udf(returnType=FloatType())
def udf_dew_point(temp, humidity):
    return calculate_dew_point(temp, humidity)

@udf(returnType=FloatType())
def udf_wind_chill(temp, wind_speed):
    return calculate_wind_chill(temp, wind_speed)

# Perform aggregations
agg_df = weather_df.groupBy("date", "city").agg(
    first("country").alias("country"),
    avg("temperature").alias("avg_temperature"),
    min("temp_min").alias("min_temperature"),
    max("temp_max").alias("max_temperature"),
    sum("rain_1h").alias("total_rainfall"),
    avg("cloud_coverage").alias("avg_cloud_coverage"),
    avg("humidity").alias("humidity"),
    avg("wind_speed").alias("wind_speed"),
    first("feels_like").alias("feels_like"),
    first("visibility").alias("visibility"),
    first("wind_deg").alias("wind_deg"),
    first("description").alias("description"),
    first("timestamp").alias("timestamp")
)

# Apply UDFs to calculate metrics
result_df = agg_df.withColumn("heat_index", udf_heat_index(col("avg_temperature"), col("humidity"))) \
                  .withColumn("dew_point", udf_dew_point(col("avg_temperature"), col("humidity"))) \
                  .withColumn("wind_chill", udf_wind_chill(col("avg_temperature"), col("wind_speed")))

# Initialize Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Function to save data to Redis
def save_to_redis(row):
    
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    data = {
        "city": row.city,
        "country": row.country,
        "avg_temperature": row.avg_temperature,
        "min_temperature": row.min_temperature,
        "max_temperature": row.max_temperature,
        "total_rainfall": row.total_rainfall,
        "avg_cloud_coverage": row.avg_cloud_coverage,
        "humidity": row.humidity,
        "wind_speed": row.wind_speed,
        "feels_like": row.feels_like,
        "visibility": row.visibility,
        "wind_deg": row.wind_deg,
        "description": row.description,
        "timestamp": row.timestamp,
        "heat_index": row.heat_index,
        "dew_point": row.dew_point,
        "wind_chill": row.wind_chill
    }
    print(row)
    # Store the data as a JSON string in Redis with a unique key
    redis_key = f"weather:{row.city}"
    redis_client.set(redis_key, json.dumps(data))

def process_batch(batch_df, batch_id):
    batch_df.foreach(save_to_redis)

query = result_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()


# Await termination of the query
query.awaitTermination()