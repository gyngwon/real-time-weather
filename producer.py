import requests
import time
from datetime import datetime
from kafka import KafkaProducer
import json
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get the API key and base URL from environment variables
API_KEY = os.getenv('API_KEY')
BASE_URL = os.getenv('BASE_URL')
cities = [
    'Seoul', 'New York', 'London', 'Tokyo', 'Paris',
    'Mumbai', 'Sydney', 'Beijing', 'Berlin', 'Moscow',
    'Rio de Janeiro', 'Cape Town', 'Toronto', 'Mexico City', 'Dubai',
    'Singapore', 'Bangkok', 'Istanbul', 'Rome', 'Los Angeles'
]

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # or 'kafka:9092' if running in Docker
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather_data(city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        weather_data = response.json()
        # Prepare data for Kafka
        data = {
            'city': weather_data['name'],
            'country': weather_data['sys']['country'],
            'temperature': weather_data['main']['temp'],
            'feels_like': weather_data['main']['feels_like'],
            'temp_min': weather_data['main']['temp_min'],
            'temp_max': weather_data['main']['temp_max'],
            'pressure': weather_data['main']['pressure'],
            'humidity': weather_data['main']['humidity'],
            'visibility': weather_data.get('visibility', 'N/A'),
            'wind_speed': weather_data['wind']['speed'],
            'wind_deg': weather_data['wind']['deg'],
            'rain_1h': weather_data.get('rain', {}).get('1h', 0),
            'cloud_coverage': weather_data['clouds']['all'],
            'description': weather_data['weather'][0]['description'],
            'icon': weather_data['weather'][0]['icon'],
            'timestamp': datetime.now().isoformat()
        }
        # Send data to Kafka topic
        producer.send('weather', value=data)
        print(f"Sent data to Kafka: {data}")
    else:
        print(f"Error: Unable to fetch weather data for {city} (Status code: {response.status_code})")

def main():
    while True:
        for city in cities:
            fetch_weather_data(city)
            time.sleep(1)  # short delay between requests
        time.sleep(10)  # 5 minutes

if __name__ == "__main__":
    main()
