from confluent_kafka import Producer
import json
import time 
import random
import yaml

# Load configuration from YAML file
with open("producer_config.yaml", "r") as f:  # Replace with your config file path
    config = yaml.safe_load(f)
    producer_config = config["producer"]

# Create a Kafka producer
producer = Producer(producer_config)

# Reports of the message sending status
def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (Exception): The error that occurred on None on success.
        msg (Message): The message
    Returns:
        None
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Function to generate weather data
def generate_weather_data(location):
    """
    Generates weather data for a given location.
    Args:
        location (str): The location for which weather data is generated.
    Returns:
        dict: A dictionary containing weather data.
    """
    return {
        "location": location,
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(40, 60), 2),
        "timestamp": int(time.time())
    }

try:
    while True:
        weather_data = generate_weather_data("location_3")
        producer.produce('weather_data', 
                            key="location_3", 
                            value=json.dumps(weather_data).encode('utf-8'), 
                            callback=delivery_report)
        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("producer stopped")
