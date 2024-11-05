from flask import Flask, jsonify, request, render_template
from datetime import datetime
import redis
import json

# Flask application setup
app = Flask(__name__)

# Redis client configuration
try:
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping()  # Check if Redis is available
except redis.ConnectionError:
    print("Could not connect to Redis. Please check if the Redis server is running.")

@app.route('/')
def index():
    return render_template('index.html')  # Render the index page with city selection

@app.route('/weather', methods=['GET'])
def get_weather():
    city = request.args.get('city')  # Get the city from the query parameters
    if city:
        # Fetch weather data for the specified city from Redis
        redis_key = f"weather:{city}"
        weather_data = redis_client.get(redis_key)
        
        if weather_data:
            weather_dict = json.loads(weather_data)
            # Convert timestamp to desired format
            timestamp = datetime.fromisoformat(weather_dict['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            weather_dict['timestamp'] = timestamp  # Update the timestamp format
            return render_template('weather.html', weather=weather_dict), 200
        else:
            return jsonify({"error": "No data found for this city."}), 404
    return jsonify({"error": "City not specified."}), 400

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=8081)
