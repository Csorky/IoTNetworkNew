from prometheus_client import start_http_server, Counter, Gauge
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import *
from pyspark.sql.streaming import DataStreamWriter
import threading

# Initialize Spark Session
def initialize_spark():
    spark = SparkSession.builder \
        .appName("IoTDataProcessing") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.instances", "4") \
        .getOrCreate()
    return spark

# Define Prometheus metrics
src_ip_counter = Counter('src_ip_count', 'Number of occurrences of each source IP', ['src_ip', 'device'])
flow_id_counter = Counter('flow_id_count', 'Number of processed Flow IDs', ['flow_id', 'device'])
processed_records = Counter('processed_records_total', 'Total number of records processed', ['device'])
processing_latency = Gauge('processing_latency', 'Latency in processing records', ['device'])


# Start Prometheus metrics server
def start_prometheus_server():
    threading.Thread(target=start_http_server, args=(7000,), daemon=True).start()
    print("Prometheus server started on port 8000")

# Function to process and record metrics
def record_metrics(batch_df, epoch_id, device):
    record_count = batch_df.count()
    print(f"Processing {record_count} records for {device}")  # Debugging log
    processed_records.labels(device=device).inc(record_count)  # Increment total processed records
    
    # Simulated processing latency
    latency = 0.5  # Replace this with actual latency calculation if needed
    print(f"Setting latency for {device}: {latency}")
    processing_latency.labels(device=device).set(latency)
    
    for row in batch_df.collect():  # Be cautious with collect on large datasets
        if row["Src_IP"]:
            print(f"Updating src_ip_counter for {row['Src_IP']} on {device}")  # Debugging log
            src_ip_counter.labels(src_ip=row["Src_IP"], device=device).inc(5)
        if row["Flow_ID"]:
            print(f"Updating flow_id_counter for {row['Flow_ID']} on {device}")  # Debugging log
            flow_id_counter.labels(flow_id=row["Flow_ID"], device=device).inc(1)

def initialize_metrics(device_list):
    for device in device_list:
        processed_records.labels(device=device)
        processing_latency.labels(device=device)
        for ip in ["192.168.0.13", "192.168.0.24", "192.168.0.16"]:  # Known IPs
            src_ip_counter.labels(src_ip=ip, device=device)
        flow_id_counter.labels(flow_id="dummy", device=device)  # Initialize with dummy value



def main():
    spark = initialize_spark()

    spark.sparkContext.setLogLevel("WARN")
    
    # Define the schema based on your dataset fields
    schema = StructType([
        StructField("Flow_ID", StringType(), True),
        StructField("Src_IP", StringType(), True),
        StructField("Src_Port", IntegerType(), True),
        StructField("Dst_IP", StringType(), True),
        StructField("Dst_Port", IntegerType(), True),
        StructField("Protocol", StringType(), True),
        StructField("Label", StringType(), True),
        StructField("Cat", StringType(), True),
        StructField("Sub_Cat", StringType(), True),
        StructField("Timestamp", StringType(), True) 
    ])

    # Read data from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "iot_topic") \
        .load()


    # Parse the "value" column as JSON and extract the fields using the schema
    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    # Start Prometheus metrics server
    start_prometheus_server()


    # Define device IPs (known IPs for each device)
    device_ips = ['192.168.0.13', '192.168.0.24', '192.168.0.16'] 

    initialize_metrics(["device_1", "device_2", "device_3"])

    # Filter the DataFrame by IP and create separate streams for each device
    df_device_1 = df_parsed.filter(col("Src_IP") == device_ips[0])
    df_device_2 = df_parsed.filter(col("Src_IP") == device_ips[1])
    df_device_3 = df_parsed.filter(col("Src_IP") == device_ips[2])

    # Set checkpoint directory for each stream (you can use different directories if needed)
    checkpoint_dir = "/tmp/spark_checkpoint"  # You can choose any directory here

    # Write the streams to different outputs (e.g., console or Elasticsearch) for each device
    query_device_1 = df_device_1.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: record_metrics(df, epoch_id, "device_1")) \
        .format("console") \
        .option("checkpointLocation", checkpoint_dir + "/device_1") \
        .start()

    query_device_2 = df_device_2.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: record_metrics(df, epoch_id, "device_2")) \
        .format("console") \
        .option("checkpointLocation", checkpoint_dir + "/device_2") \
        .start()

    query_device_3 = df_device_3.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: record_metrics(df, epoch_id, "device_3")) \
        .format("console") \
        .option("checkpointLocation", checkpoint_dir + "/device_3") \
        .start()


    # Await termination of the streams
    query_device_1.awaitTermination()
    # query_device_2.awaitTermination()
    # query_device_3.awaitTermination()

if __name__ == "__main__":
    main()
