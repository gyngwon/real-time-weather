from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import calculate  # Import your calculation functions

def process_row(row):
    """
    Kafka에서 수신한 데이터를 계산 모듈로 전달하여 처리
    """
    data = {
        "city": row.city,
        "country": row.country,
        "temperature": row.temperature,
        "feels_like": row.feels_like,
        "temp_min": row.temp_min,
        "temp_max": row.temp_max,
        "pressure": row.pressure,
        "humidity": row.humidity,
        "visibility": row.visibility,
        "wind_speed": row.wind_speed,
        "wind_deg": row.wind_deg,
        "rain_1h": row.rain_1h,
        "cloud_coverage": row.cloud_coverage,
        "description": row.description,
        "icon": row.icon,
        "timestamp": row.timestamp
    }

    # Update the function calls to match the function names in calculate.py
    heat_index = calculate.calculate_heat_index(data["temperature"], data["humidity"])  # Updated function name
    dew_point = calculate.calculate_dew_point(data["temperature"], data["humidity"])  # Updated function name
    wind_chill = calculate.calculate_wind_chill(data["temperature"], data["wind_speed"])  # Updated function name
    total_rainfall = calculate.calculate_total_rainfall([data["rain_1h"]])  # Wrap in a list
    avg_cloud_coverage = calculate.calculate_average_cloud_coverage([data["cloud_coverage"]])  # Wrap in a list

    # Print formatted data
    print(f"City: {data['city']} | Country: {data['country']}")
    print(f"Temperature: {data['temperature']:.2f}°C | Feels Like: {data['feels_like']:.2f}°C | "
          f"Min Temp: {data['temp_min']:.2f}°C | Max Temp: {data['temp_max']:.2f}°C")
    print(f"Pressure: {data['pressure']:.2f} hPa | Humidity: {data['humidity']}% | "
          f"Visibility: {data['visibility']} meters")
    print(f"Wind Speed: {data['wind_speed']:.2f} m/s | Wind Degree: {data['wind_deg']}°")
    print(f"Rainfall (Last 1h): {data['rain_1h']:.2f} mm | Cloud Coverage: {data['cloud_coverage']}%")
    print(f"Heat Index: {heat_index:.2f}°C | Dew Point: {dew_point:.2f}°C | Wind Chill: {wind_chill:.2f}°C")
    print(f"Total Rainfall: {total_rainfall:.2f} mm | Avg Cloud Coverage: {avg_cloud_coverage:.2f}%\n")
    
    # Call the processing function from calculate.py
    calculate.process_weather_data(data)

def main():
    spark = SparkSession.builder \
        .appName("KafkaStreamReader") \
        .master("local[*]") \
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
        StructField("temp_min", FloatType(), True),
        StructField("temp_max", FloatType(), True),
        StructField("pressure", FloatType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("wind_deg", IntegerType(), True),
        StructField("rain_1h", FloatType(), True),
        StructField("cloud_coverage", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    
    # Parse the JSON value into a structured DataFrame
    parsed_stream_df = kafka_stream_df.select(from_json(col("value").cast("string"), weather_schema).alias("data"))
    
    # Select fields from the parsed DataFrame
    parsed_stream_df = parsed_stream_df.select("data.*")

    # Write the output to the console
    query = parsed_stream_df.writeStream \
        .foreach(process_row) \
        .outputMode("update") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
