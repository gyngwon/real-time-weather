import json
import pymysql
from kafka import KafkaConsumer
from datetime import datetime

# Kafka server information
KAFKA_BROKER = 'localhost:29092' 

# MySQL connection details
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "wony123"
MYSQL_DB = "weather_db"

# Kafka Consumer setup
consumer = KafkaConsumer(
    'metricsWeather',  # Topic for individual weather metrics
    'aggregatedWeather',  # Topic for aggregated weather data
    'weatherDetails',  # Topic for detailed weather data
    bootstrap_servers=KAFKA_BROKER,
    group_id='weather_consumer_group',  # Consumer group ID
    auto_offset_reset='earliest',  # Start reading from the beginning if no offset exists
)

# MySQL connection setup
connection = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB
)

# Function to insert metricsWeather data into MySQL
def insert_metrics_weather(data):
    cursor = connection.cursor()
    query = """INSERT INTO metricsWeather (city, timestamp, heat_index, dew_point, wind_chill)
               VALUES (%s, %s, %s, %s, %s)"""
    cursor.execute(query, (data['city'], data['timestamp'], data['heat_index'], data['dew_point'], data['wind_chill']))
    connection.commit()

# Function to insert aggregatedWeather data into MySQL with corrected handling of nested 'window' field
def insert_aggregated_weather(data):
    # Ensure that all required keys exist in the data
    required_keys = ['city', 'window', 'avg_temperature', 
                     'min_temperature', 'max_temperature', 'avg_humidity', 
                     'min_humidity', 'max_humidity']

    
    # Check if all required keys are present (adjust for the nested 'window' field)
    if all(key in data for key in required_keys) and 'start' in data['window'] and 'end' in data['window']:
        cursor = connection.cursor()
        query = """INSERT INTO aggregatedWeather (city, window_start, window_end, avg_temperature, 
                   min_temperature, max_temperature, avg_humidity, min_humidity, max_humidity)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        # Extract 'start' and 'end' from the 'window' field
        window_start = data['window']['start']
        window_end = data['window']['end']

        # Insert into the database
        cursor.execute(query, (data['city'], window_start, window_end, data['avg_temperature'],
                               data['min_temperature'], data['max_temperature'], data['avg_humidity'],
                               data['min_humidity'], data['max_humidity']))
        connection.commit()
    else:
        # Log the error if required keys are missing
        missing_keys = [key for key in required_keys if key not in data]
        print(f"Error: Missing keys in data - {missing_keys}")

# Function to insert weatherDetails data into MySQL
def insert_weather_details(data):
    cursor = connection.cursor()

    # Ensure all required fields are present
    required_fields = ['city', 'country', 'temperature', 'feels_like', 'temp_min', 'temp_max', 
                       'pressure', 'humidity', 'visibility', 'wind_speed', 'wind_deg', 'rain_1h', 
                       'cloud_coverage', 'description']

    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        print(f"Error: Missing fields - {missing_fields}")
        return

    # SQL query with placeholders for each field
    query = """INSERT INTO weatherDetails (city, country, temperature, feels_like, temp_min, temp_max, 
               pressure, humidity, visibility, wind_speed, wind_deg, rain_1h, cloud_coverage, description,
               timestamp)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    
    # Arguments should be passed as a tuple
    arguments = (data['city'], data['country'], data['temperature'], data['feels_like'], 
                 data['temp_min'], data['temp_max'], data['pressure'], data['humidity'], 
                 data['visibility'], data['wind_speed'], data['wind_deg'], data['rain_1h'], 
                 data['cloud_coverage'], data['description'],  data['timestamp'])

    try:
        cursor.execute(query, arguments)
        connection.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")



# Process messages from Kafka
for message in consumer:
    message_value = message.value.decode('utf-8')
    data = json.loads(message_value)  

    # Handle metricsWeather data
    if message.topic == 'metricsWeather':
        insert_metrics_weather(data)
    # Handle aggregatedWeather data
    elif message.topic == 'aggregatedWeather':
        insert_aggregated_weather(data)
    # Handle weatherDetails data
    elif message.topic == 'weatherDetails':
        insert_weather_details(data)

# Close MySQL connection when done
connection.close()
