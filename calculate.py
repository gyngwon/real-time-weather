import math
import json

# Function to calculate the heat index
def calculate_heat_index(temperature, humidity):
    # Set default values if None
    temperature = temperature if temperature is not None else 0.0
    humidity = humidity if humidity is not None else 0.0

    HI = 0.5 * (temperature + 61.0 + ((temperature - 68.0) * 1.2) + (humidity * 0.094))
    return HI

# Function to calculate the dew point
def calculate_dew_point(temperature, humidity):
    a = 17.27
    b = 243.12
    alpha = ((a * temperature) / (b + temperature)) + math.log(humidity / 100.0)
    dew_point = (b * alpha) / (a - alpha)
    # dew_point = temperature - ((100 - humidity)/5)
    return dew_point

# Function to calculate wind chill
# This formula is based on guidelines provided by the North American meteorological community for calculating wind chill.
def calculate_wind_chill(temperature, wind_speed):
    if temperature <= 10 and wind_speed > 4.8:
        wind_chill = (13.12 + 0.6215 * temperature - 11.37 * (wind_speed ** 0.16)
                      + 0.3965 * temperature * (wind_speed ** 0.16))
        return wind_chill
    else:
        return temperature  # No wind chill effect


# Function to process weather data
def process_weather_data(data):
    # Extract relevant fields from the incoming data
    temperature = data['temperature']
    humidity = data['humidity']
    wind_speed = data['wind_speed']

    
    # Update global lists for calculations
    global temperatures, humidities, wind_speeds
    temperatures.append(temperature)
    humidities.append(humidity)
    wind_speeds.append(wind_speed)

    # Perform calculations
    heat_index = calculate_heat_index(temperature, humidity)
    dew_point = calculate_dew_point(temperature, humidity)
    wind_chill = calculate_wind_chill(temperature, wind_speed)

# Example list to hold previous values
city = []
temperatures = []
humidities = []
wind_speeds = []

