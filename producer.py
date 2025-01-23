import requests
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

API_KEY = '38764b6b9b354ed35250a90b4b2f1d64'
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'
cities = [
    'Bolzano', 'Seoul', 'London', 'New York', 'Paris',
    'Mumbai', 'Sydney', 'Beijing', 'Berlin', 'Moscow',
    'Rio de Janeiro', 'Cape Town', 'Toronto', 'Mexico City', 'Dubai',
    'Singapore', 'Bangkok', 'Istanbul', 'Tokyo', 'Los Angeles'
]

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
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
        # Get the timezone offset
        timezone_offset = weather_data['timezone']

        # Add the timezone_offset to the UTC time to calculate the local time
        utc_time = datetime.utcnow()
        local_time = utc_time + timedelta(seconds=timezone_offset)

        # Convert the local time to a string (omit decimal points from the seconds)
        local_time_str = local_time.strftime('%Y-%m-%dT%H:%M:%S')

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
            'timestamp': local_time_str  # 현지 시간을 timestamp로 저장
        }

        # Send data to the "weatherDetails" Kafka topic
        producer.send('weatherDetails', value=data)
        print(f"Sent data to Kafka topic 'weatherDetails': {data}")
    else:
        print(f"Error: Unable to fetch weather data for {city} (Status code: {response.status_code})")


def main():
    while True:
        for city in cities:
            fetch_weather_data(city)
            time.sleep(1)  # short delay between requests
        time.sleep(300)  # sleep for 5 minutes before fetching data again

if __name__ == "__main__":
    main()
