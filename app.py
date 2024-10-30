from flask import Flask, jsonify, request
import redis
import json

# Flask 애플리케이션 생성
app = Flask(__name__)

# Redis 클라이언트 설정
try:
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping()  # Check if Redis is available
except redis.ConnectionError:
    print("Could not connect to Redis. Please check if the Redis server is running.")

@app.route('/')
def index():
    return "Welcome to the Flask application!"

@app.route('/weather/<city>', methods=['GET'])
def get_weather(city):
    # Redis에서 특정 도시의 날씨 데이터 가져오기
    redis_key = f"weather:{city}"
    weather_data = redis_client.get(redis_key)
    
    if weather_data:
        return jsonify({"city": city, "data": json.loads(weather_data)}), 200
    else:
        return jsonify({"error": "No data found for this city."}), 404

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=8081)
