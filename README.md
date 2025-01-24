# Real-Time Weather Monitoring System

This project is a real-time weather monitoring system that fetches weather data from the OpenWeatherMap API, processes it using Apache Spark, and streams the results through Kafka. The processed data includes calculated metrics such as heat index, dew point, and wind chill, and is stored in MySQL and Kafka topics for analysis and visualization.

**GitHub Repository**: [https://github.com/gyngwon/real-time-weather.git](https://github.com/gyngwon/real-time-weather.git)

## Features

- **Data Collection**: Fetch real-time weather data from multiple cities using the OpenWeatherMap API.
- **Data Processing**: Perform calculations such as heat index, dew point, and wind chill using Apache Spark.
- **Data Streaming**: Stream raw and processed weather data through Kafka.
- **Data Aggregation**: Aggregate weather metrics (e.g., average, minimum, and maximum values) over a time window.
- **Data Storage**: Persist processed and aggregated metrics in MySQL and Kafka topics.
- **Visualization**: Monitor and debug Kafka topics using Kafka-UI.

## System Architecture

<img width="1048" alt="Image" src="https://github.com/user-attachments/assets/85c2448c-8c1f-4c3f-aeb4-f40bf67764c4" />
- **OpenWeather API**: This is the primary source for fetching real-time weather data, including temperature, humidity, and wind speed.
- **Kafka**: Acts as a distributed message broker to stream the collected weather data, allowing for efficient handling of data from multiple cities.
- **Apache Spark Streaming**: Processes the data streamed from Kafka in real-time, calculating additional metrics such as heat index and wind chill.
- **MySQL Database**: Stores both raw and processed weather data for historical analysis and retrieval.
- **Streamlit**: Provides an interactive dashboard for visualizing the processed weather data, allowing users to view current conditions and metrics easily.

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- OpenWeatherMap API Key

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/real-time-weather-monitoring.git
   cd real-time-weather-monitoring
   ```
2. Update the OpenWeatherMap API key in `producer.py`:

```python
export API_KEY='your_api_key_here' 
```
3. Create Kafka topics: Run the following script to create Kafka topics:
```bash
./create-kafka-topics.sh
```
4. Start the services using Docker Compose:
```bash
docker-compose up -d
```
5. Install Python dependencies:
```bash
pip install -r requirements.txt
```
## Components

### `docker-compose.yaml`
Defines the infrastructure components such as MySQL, Kafka, Zookeeper, Spark, and Kafka-UI. Key configurations include:

- Kafka Topics: `weatherDetails`, `metricsWeather`, and `aggregatedWeather`.
- Spark Workers: Configured with 4 cores and 1 GB memory.

### `producer.py`
Fetches weather data from the OpenWeatherMap API and streams it to the `weatherDetails` Kafka topic. Key features:

- Handles timezone offsets and local time calculation.
- Serializes weather data into JSON format before publishing.

### `data_stream.py`
Processes the weather data using Apache Spark. Key functionalities:

- Reads raw data from the `weatherDetails` topic.
- Calculates heat index, dew point, and wind chill.
- Aggregates metrics over 10-minute windows.
- Streams processed data to `metricsWeather` and `aggregatedWeather` Kafka topics.

### `calculate.py`
Implements functions to calculate:

- Heat Index
- Dew Point
- Wind Chill

### `requirements.txt`
Lists required Python packages, including:

- `requests`
- `kafka-python`
- `pyspark`
- `pymysql`
- `pandas`

## Usage

### 1. Run the Producer: Start fetching and streaming weather data:

```bash
python producer.py
```
### 2. Run the Data Stream Processor: Start Spark streaming job:
```bash
spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.redislabs:spark-redis_2.12:3.0.0 data_stream.py
```
### 3. Monitor Kafka Topics

- Access Kafka-UI at [http://localhost:8080](http://localhost:8080).
- View real-time data in topics: `weatherDetails`, `metricsWeather`, `aggregatedWeather`.

### 4. Save the data to MySQL 
```bash
python mysql.py
```
### 5. Run the Streamlit dashboard
```bash
streamlit run app.py
```

## Data Schema

### Raw Weather Data (`weatherDetails`):
- `city`: City name
- `country`: Country code
- `temperature`: Current temperature in Celsius (°C)  
- `humidity`: Current humidity percentage (%)  
- `timestamp`: Local timestamp of data collection  
- `Feels like`: Perceived temperature (°C), considering humidity and wind effects  
- `Min/Max temperature`: Expected minimum and maximum temperatures for the day (°C)  
- `Pressure`: Atmospheric pressure (hPa)  
- `Visibility`: Maximum visible distance (in meters or kilometers)  
- `Wind speed`: Wind speed (in m/s or km/h)
- `Wind degree (wind_deg`: Direction of the wind, measured in degrees (0° = North, 90° = East, etc.)  
- `Rain (rain_1h`: Amount of rain in the last hour (in millimeters)  
- `Cloud coverage (cloud_coverage`: Percentage of the sky covered by clouds  
- `Description`: General weather description (e.g., "clear sky", "rainy")  
- `Icon`: Weather icon code (used for visual representation) 

### Processed Metrics (`metricsWeather`):
- `heat_index`: Calculated heat index
- `dew_point`: Calculated dew point
- `wind_chill`: Calculated wind chill

### Aggregated Metrics (`aggregatedWeather`):
- Average, minimum, and maximum values for temperature and humidity over 1-hour windows.
