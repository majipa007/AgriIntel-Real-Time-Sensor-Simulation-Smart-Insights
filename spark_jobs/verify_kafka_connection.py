import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Kafka connection details
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"

def main():
    """
    Main function to initialize Spark, connect to Kafka, and process the stream.
    """
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("KafkaWeatherStreamVerifier") \
        .master("local[*]") \
        .getOrCreate()

    # Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session created successfully.")

    print(f"Connecting to Kafka topic '{KAFKA_TOPIC}' at '{KAFKA_BROKER}'...")
    # Read from Kafka topic as a streaming DataFrame
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Successfully connected to Kafka. Reading messages...")

    # Define the schema for the incoming JSON data based on the producer
    schema = StructType([
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    # Parse the JSON data from the 'value' column
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                        .select("data.*")

    # Print the streaming data to the console
    query = parsed_df.writeStream.outputMode("append").format("console").start()

    print("Streaming query started. Waiting for data... (Press Ctrl+C to stop)")
    query.awaitTermination()

if __name__ == "__main__":
    main()