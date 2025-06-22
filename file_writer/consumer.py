from confluent_kafka import Consumer, KafkaError, KafkaException
import json 
import yaml

# Load configuration from yaml
with open("consumer_config.yaml", "r") as f:
    config = yaml.safe_load(f)
    consumer_config = config["consumer"]   
    topics = config["topics"]

# Create a Karka consumer
consumer = Consumer(consumer_config)
 
try:
    consumer.subscribe(topics)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        print(f"Received message: {msg.value().decode('utf-8')}")
        try:
            value = json.loads(msg.value().decode('utf-8'))
            print(value)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print("Consumer closed.")