from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def main():
    spark = SparkSession.builder \
        .appName("KafkaStreamReader") \
        .getOrCreate()

    kafka_topic = "weather"
    
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Define the schema for the JSON data
    weather_schema = StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("temperature", FloatType(), True),
        StructField("feels_like", FloatType(), True),
        StructField("description", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    
    # Parse the JSON value into a structured DataFrame
    parsed_stream_df = kafka_stream_df.select(from_json(col("value").cast("string"), weather_schema).alias("data"))
    
    # Select fields from the parsed DataFrame
    parsed_stream_df = parsed_stream_df.select("data.*")

    # Write the output to the console
    query = parsed_stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
