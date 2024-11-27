from kafka import KafkaProducer
import pandas as pd
import json
import time
import sys
from kafka.errors import NoBrokersAvailable

def create_producer():
    # Initialize Kafka Producer with retry logic
    max_retries = 10
    retry_delay = 5  # seconds
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka broker.")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt} of {max_retries}: Kafka broker not available. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    else:
        print("Failed to connect to Kafka broker after multiple attempts. Exiting.")
        sys.exit(1)

def stream_device_data(df, device_ip, producer):
    # Filter data for the specific device by IP address
    device_data = df[df['Src_IP'] == device_ip]
    
    # Stream the filtered data for the device to the Kafka topic
    for _, row in device_data.iterrows():
        data = row.to_dict()
        producer.send('iot_topic', value=data)
        time.sleep(1)  # Simulate real-time streaming
    
    print(f"Data streaming for device with IP {device_ip} completed.")

def main():
    # Define the known IP addresses for each device
    device_ips = ['192.168.0.13', '192.168.0.24', '192.168.0.16'] 
    # Read the dataset
    df = pd.read_csv('/data/iot_network_intrusion_dataset.csv')
    
    # Create Kafka Producers for each device
    producer_device_1 = create_producer()
    producer_device_2 = create_producer()
    producer_device_3 = create_producer()

    # Stream data for each device separately using their respective IPs
    stream_device_data(df, device_ip=device_ips[0], producer=producer_device_1)  # IP of Device 1
    stream_device_data(df, device_ip=device_ips[1], producer=producer_device_2)  # IP of Device 2
    stream_device_data(df, device_ip=device_ips[2], producer=producer_device_3)  # IP of Device 3

    # Close the producers after the data streaming
    producer_device_1.flush()
    producer_device_1.close()
    producer_device_2.flush()
    producer_device_2.close()
    producer_device_3.flush()
    producer_device_3.close()

if __name__ == "__main__":
    main()
