# producer/kafka_producer.py

from kafka import KafkaProducer
import pandas as pd
import json
import time
import sys
from kafka.errors import NoBrokersAvailable

def create_producer():
    """
    Initialize Kafka Producer with retry logic and essential configurations.
    """
    max_retries = 10
    retry_delay = 5  # seconds
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',               # Ensure all replicas acknowledge
                retries=5,                # Number of retries for failed sends
                retry_backoff_ms=100      # Backoff time between retries
            )
            print("Connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt} of {max_retries}: Kafka broker not available. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    else:
        print("Failed to connect to Kafka broker after multiple attempts. Exiting.")
        sys.exit(1)

def stream_data(df, producer, batch_size=10, delay=1):
    """
    Stream data to Kafka at a controlled rate.

    :param df: Pandas DataFrame containing the dataset.
    :param producer: KafkaProducer instance.
    :param batch_size: Number of records to send per batch.
    :param delay: Delay in seconds between batches.
    """
    total_records = len(df)
    print(f"Streaming {total_records} records to Kafka.")

    for start in range(0, total_records, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end].to_dict(orient='records')
        for record in batch:
            producer.send('iot_topic', value=record)
        producer.flush()
        print(f"Sent batch {start//batch_size + 1} ({len(batch)} records).")
        time.sleep(delay)  # Maintain 1-second interval between batches

    print("Data streaming to Kafka completed.")

def main():
    """
    Main function to initiate Kafka producer and start streaming data.
    """
    # Read the dataset
    try:
        df = pd.read_csv('/data/iot_network_intrusion_dataset.csv')
    except FileNotFoundError:
        print("Dataset file not found. Ensure the CSV is available at /data/iot_network_intrusion_dataset.csv")
        sys.exit(1)

    # Create a Kafka Producer
    producer = create_producer()

    # Stream data to Kafka
    stream_data(df, producer, batch_size=10, delay=1)  # 10 records per second

    # Close the producer after data streaming
    producer.flush()
    producer.close()
    print("Kafka producer has been closed.")

if __name__ == "__main__":
    main()
