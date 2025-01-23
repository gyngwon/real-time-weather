# Real-Time Weather Monitoring System

This project is a real-time weather monitoring system that fetches weather data from the OpenWeatherMap API, processes it using Apache Spark, and streams the results through Kafka. The processed data includes calculated metrics such as heat index, dew point, and wind chill, and is stored in MySQL and Kafka topics for analysis and visualization.

## Features

- **Data Collection**: Fetch real-time weather data from multiple cities using the OpenWeatherMap API.
- **Data Processing**: Perform calculations such as heat index, dew point, and wind chill using Apache Spark.
- **Data Streaming**: Stream raw and processed weather data through Kafka.
- **Data Aggregation**: Aggregate weather metrics (e.g., average, minimum, and maximum values) over a time window.
- **Data Storage**: Persist processed and aggregated metrics in MySQL and Kafka topics.
- **Visualization**: Monitor and debug Kafka topics using Kafka-UI.

## System Architecture

The system consists of the following components:

- **MySQL**: Stores weather data for further analysis.
- **Kafka**: Streams raw and processed weather data.
- **Apache Spark**: Processes real-time weather data and performs metric calculations.
- **Zookeeper**: Manages Kafka brokers.
- **Kafka-UI**: Provides a web interface to monitor Kafka topics.

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
API_KEY = 'your_api_key_here'
```
3. Start the services using Docker Compose:
```bash
docker-compose up -d
```
4. Install Python dependencies:
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

## Data Schema

### Raw Weather Data (`weatherDetails`):
- `city`: City name
- `country`: Country code
- `temperature`: Current temperature in Celsius
- `humidity`: Current humidity percentage
- `timestamp`: Local timestamp of data collection
- Other metrics: Feels like, min/max temperature, pressure, visibility, wind speed, etc.

### Processed Metrics (`metricsWeather`):
- `heat_index`: Calculated heat index
- `dew_point`: Calculated dew point
- `wind_chill`: Calculated wind chill

### Aggregated Metrics (`aggregatedWeather`):
- Average, minimum, and maximum values for temperature and humidity over 10-minute windows.
