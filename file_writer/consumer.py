from confluent_kafka import Consumer, KafkaError, KafkaException
import json 
import yaml
import pandas as pd
import os
from datetime import datetime
import time
from zoneinfo import ZoneInfo


# Defining the desired timezone ( IST )
IST = ZoneInfo('Asia/Kolkata')

# Load configuration from yaml
with open("consumer_config.yaml", "r") as f:
    config = yaml.safe_load(f)
    consumer_config = config["consumer"]   
    topics = config["topics"]
    storage_config = config.get("storage", {})



# Get storage settings or use default fromt the config
BASE_PATH = storage_config.get("base_path", "data/raw")
BATCH_SIZE = storage_config.get("batch_size", 100)
message_batch = []



# Helper function to write Data
def write_batch_to_parquet(batch):
    """
    Converts a batch of message to Pandas DataFrame and writes data-partitioned Parquet file.
    """
    if not batch:
        return
    try: 
        # Get current date for partition
        now = datetime.now(tz=IST)
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour 
        
        # Define the output path with Hive-style paritioning
        output_dir = os.path.join(BASE_PATH, str(year), str(month), str(day), str(hour))
        os.makedirs(output_dir, exist_ok=True)

        # Create a unique filemane using a milisecond timestamp
        file_timestamp = int(time.time() * 1000)
        file_path = os.path.join(output_dir, f"batch_{file_timestamp}.parquet")

        # COnvert to DataFrame and save as Parquet
        df = pd.DataFrame(batch)
        df.to_parquet(file_path, index=False)
        print(f"Wrote batch to {file_path}")
    except Exception as e:
        print(f"Error writing batch to Parquet: {e}")

# Main Consumer Logic
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
            record = json.loads(msg.value().decode('utf-8'))
            message_batch.append(record)
            
            # If Batch is full, write it to Parquet
            if len(message_batch) >= BATCH_SIZE:
                write_batch_to_parquet(message_batch)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")
except KeyboardInterrupt:
    pass
finally:
    print("writing Final batch before closing...")
    write_batch_to_parquet(message_batch)
    print("Closing consumer...")
    consumer.close()
    print("Consumer closed.")