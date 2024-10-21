from kafka import KafkaConsumer
import json

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'weather',
    bootstrap_servers='localhost:9092',  # or 'kafka:9092' if running in Docker
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

def main():
    print("Listening for messages on 'weather' topic...")
    for message in consumer:
        print(f"Received message: {message.value}")

if __name__ == "__main__":
    main()
