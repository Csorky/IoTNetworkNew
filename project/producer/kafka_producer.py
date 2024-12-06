# producer/kafka_producer.py

from kafka import KafkaProducer
import pandas as pd
import json
import time
import sys
from kafka.errors import NoBrokersAvailable
from threading import Thread

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

def stream_data(df, producer, topic, batch_size=10, delay=1):
    """
    Stream data to Kafka at a controlled rate.

    :param df: Pandas DataFrame containing the dataset.
    :param producer: KafkaProducer instance.
    :param topic: Kafka topic to stream data to.
    :param batch_size: Number of records to send per batch.
    :param delay: Delay in seconds between batches.
    """
    total_records = len(df)
    print(f"Streaming {total_records} records to Kafka topic '{topic}'.")

    for start in range(0, total_records, batch_size):
        end = start + batch_size
        batch = df.iloc[start:end].to_dict(orient='records')
        for record in batch:
            producer.send(topic, value=record)
        producer.flush()
        print(f"Sent batch {start//batch_size + 1} ({len(batch)} records) to topic '{topic}'.")
        time.sleep(delay)  # Maintain interval between batches

    print(f"Data streaming to Kafka topic '{topic}' completed.")

def stream_full_data():
    """
    Function to stream the full dataset to 'iot_topic_full'.
    """
    try:
        df_full = pd.read_csv('/data/iot_network_intrusion_dataset_full.csv')
    except FileNotFoundError:
        print("Full dataset file not found. Ensure the CSV is available at /data/iot_network_intrusion_dataset_full.csv")
        sys.exit(1)

    producer_full = create_producer()
    try:
        stream_data(df_full, producer_full, topic='iot_topic_full', batch_size=50, delay=0.1)  # Faster rate
    finally:
        producer_full.flush()
        producer_full.close()
        print("Producer for 'iot_topic_full' has been closed.")

def stream_filtered_data():
    """
    Function to stream the filtered dataset to 'iot_topic'.
    """
    try:
        df_filtered = pd.read_csv('/data/iot_network_intrusion_dataset_stream.csv')
    except FileNotFoundError:
        print("Filtered dataset file not found. Ensure the CSV is available at /data/iot_network_intrusion_dataset.csv")
        sys.exit(1)

    producer_filtered = create_producer()
    try:
        stream_data(df_filtered, producer_filtered, topic='iot_topic', batch_size=120, delay=1)  # Slower rate
    finally:
        producer_filtered.flush()
        producer_filtered.close()
        print("Producer for 'iot_topic' has been closed.")

def main():
    """
    Main function to initiate Kafka producers and start streaming data in parallel.
    """
    # Create threads for parallel streaming
    thread_full = Thread(target=stream_full_data)
    thread_filtered = Thread(target=stream_filtered_data)

    # Start threads
    thread_full.start()
    thread_filtered.start()

    # Wait for both threads to complete
    thread_full.join()
    thread_filtered.join()

    print("Both Kafka data streams have been completed.")

if __name__ == "__main__":
    main()
