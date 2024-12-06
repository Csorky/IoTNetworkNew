# pyspark/pyspark_streaming.py

from prometheus_client import start_http_server, Counter, Gauge
from pipelines.pca_cd_detector import *
from pipelines.md3_detector import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import threading
import time
import statistics
import pandas as pd

from pyspark.sql.functions import from_json, col, min as spark_min, max as spark_max, mean as spark_mean

# Initialize Spark Session
def initialize_spark():
    spark = SparkSession.builder \
        .appName("IoTDataProcessing") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.instances", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "10") \
        .getOrCreate()
    return spark

# Define Prometheus metrics
src_ip_counter = Counter('src_ip_count', 'Number of occurrences of each source IP', ['src_ip'])
flow_id_counter = Counter('flow_id_count', 'Number of processed Flow IDs', ['flow_id'])
processed_records = Counter('processed_records_total', 'Total number of records processed')
processing_latency = Gauge('processing_latency', 'Latency in processing records')

# CUSUM metrics
idle_min_gauge = Gauge('idle_min', 'Idle time in minutes for each source IP', ['src_ip'])
idle_min_mean_gauge = Gauge('idle_min_batch', 'Idle time in minutes for 10 values gor each source IP', ['src_ip'])

# Prometheus Gauges for per-device global min and max (Flow Duration)
flow_duration_gauge = Gauge('flow_duration', '', ['src_ip'])
global_min_gauge = Gauge('global_flow_duration_min', 'Global minimum Flow_Duration for each device', ['src_ip'])
global_max_gauge = Gauge('global_flow_duration_max', 'Global maximum Flow_Duration for each device', ['src_ip'])

# Initialize globals outside the functions
global_flow_duration_min = {}
global_flow_duration_max = {}

# Start Prometheus metrics server
def start_prometheus_server():
    threading.Thread(target=start_http_server, args=(7000,), daemon=True).start()
    print("Prometheus server started on port 7000")


# Main Streaming Logic
def main():
    spark = initialize_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    schema = StructType([
        StructField("Flow_ID", StringType(), True),
        StructField("Src_IP", StringType(), True),
        StructField("Src_Port", IntegerType(), True),
        StructField("Dst_IP", StringType(), True),
        StructField("Dst_Port", IntegerType(), True),
        StructField("Protocol", StringType(), True),
        StructField("Flow_Duration", FloatType(), True),
        StructField("Tot_Fwd_Pkts", IntegerType(), True),
        StructField("Tot_Bwd_Pkts", IntegerType(), True),
        StructField("TotLen_Fwd_Pkts", FloatType(), True),
        StructField("Active_Max", FloatType(), True),
        StructField("Active_Min", FloatType(), True),
        StructField("Idle_Mean", FloatType(), True),
        StructField("Idle_Std", FloatType(), True),
        StructField("Idle_Max", FloatType(), True),
        StructField("Idle_Min", FloatType(), True),
        StructField("Label", StringType(), True),
        StructField("Cat", StringType(), True),
        StructField("Sub_Cat", StringType(), True),
        StructField("Timestamp", StringType(), True)
    ])

    # maxOffsetsPerTrigger limits the number of records read from Kafka in each micro-batch.
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "iot_topic") \
        .option("maxOffsetsPerTrigger", 300) \
        .load()

    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    start_prometheus_server()

    device_ips = ['192.168.0.13', '192.168.0.24', '192.168.0.16']

    #Initialize PCA detectors
    device_pca_cd_detectors = {ip: DevicePCACDDetectors(window_size=100) for ip in device_ips}

    # Initialize MD3 detectors
    # Read initial training data from a static CSV file
    training_data_path = "/data/iot_network_intrusion_dataset_model.csv"
    device_md3_detectors = {}
    
    for ip in device_ips:
        detector = DeviceMD3Detectors()
        try:
            detector.initialize_detector(training_data_path, ip)
            device_md3_detectors[ip] = detector
            print(f"Initialized MD3 detector for device {ip}")
        except ValueError as e:
            print(f"Could not initialize MD3 detector for device {ip}: {e}")

    # Initialize dictionaries to store training data per device
    device_md3_training_data = {ip: [] for ip in device_ips}
    required_training_samples = 100  # Number of samples needed to initialize the detector

    # Dictionaries for tracking per-device global min and max
    global global_flow_duration_min, global_flow_duration_max
    global_flow_duration_min = {ip: float("inf") for ip in device_ips}
    global_flow_duration_max = {ip: float("-inf") for ip in device_ips}

    
    #  # Initial training phase
    # print("Collecting data for initial training...")
    # training_data = df_parsed.filter(df_parsed.Label.isNotNull()).toPandas()

    # if not training_data.empty:
    #     for ip in device_ips:
    #         device_data = training_data[training_data["Src_IP"] == ip]
    #         if device_data.empty:
    #             continue

    #         # Extract training features and labels
    #         X_train = device_data[["Flow_Duration", "Tot_Fwd_Pkts", "Tot_Bwd_Pkts"]].values
    #         y_train = device_data["Label"].values

    #         # Train model for each device
    #         if not device_ddm_detectors[ip].trained:
    #             device_ddm_detectors[ip].fit_initial_model(X_train, y_train)
    #             print(f"Initial model trained for device {ip}")



    def record_metrics(batch_df, epoch_id):
        global global_flow_duration_min
        global global_flow_duration_max
        print("Inside record_metrics - Global Flow_Duration:", global_flow_duration_min)
        if batch_df.rdd.isEmpty():
            return

        start_time = time.time()
        record_count = batch_df.count()
        processed_records.inc(record_count)

        # Simple statistics part: min, max, mean <- Flow Duration
        for ip in device_ips:
            device_df = batch_df.filter(col("Src_IP") == ip)
            if device_df.rdd.isEmpty():
                continue
                
            # Calculation min/max/mean for batches (micro-batch)    
            device_agg_metrics = device_df.agg(
                spark_min("Flow_Duration").alias("Flow_Duration_Min"),
                spark_max("Flow_Duration").alias("Flow_Duration_Max"),
                spark_mean("Flow_Duration").alias("Flow_Duration_Mean")
            ).collect()[0]

            print(f"Device {ip} - Flow_Duration: Min={device_agg_metrics['Flow_Duration_Min']}, "
                  f"Max={device_agg_metrics['Flow_Duration_Max']}, Mean={device_agg_metrics['Flow_Duration_Mean']:.2f}")

            # Update Prometheus (micro-batch) metrics for each device
            flow_duration_gauge.labels(src_ip=f"{ip}_min").set(device_agg_metrics["Flow_Duration_Min"])
            flow_duration_gauge.labels(src_ip=f"{ip}_max").set(device_agg_metrics["Flow_Duration_Max"])
            flow_duration_gauge.labels(src_ip=ip).set(device_agg_metrics["Flow_Duration_Mean"])
           
            batch_min_flow= device_agg_metrics["Flow_Duration_Min"]
            batch_max_flow = device_agg_metrics["Flow_Duration_Max"]
        
            # Update global min and max for this device
            if batch_min_flow is not None:
                global_flow_duration_min[ip] = min(global_flow_duration_min[ip], batch_min_flow)
            if batch_max_flow is not None:
                global_flow_duration_max[ip] = max(global_flow_duration_max[ip], batch_max_flow)

            # Log the global min and max for this device
            print(f"Device {ip} - Global Idle_Min: Min={global_flow_duration_min[ip]}, Max={global_flow_duration_max[ip]}")

            # Update Prometheus metrics for this device
            global_min_gauge.labels(src_ip=ip).set(global_flow_duration_min[ip])
            global_max_gauge.labels(src_ip=ip).set(global_flow_duration_max[ip])

        # Update Prometheus metrics using Spark's aggregations <- Total number of records by src_ips
        src_ip_counts = batch_df.groupBy("Src_IP").count().collect()
        for row in src_ip_counts:
            if row["Src_IP"]:
                src_ip_counter.labels(src_ip=row["Src_IP"]).inc(row["count"])

        # Data for Drift Detection (CUSUM + DDM)
        # Iterate over each device IP and process its data
        for ip in device_ips:
            device_df = batch_df.filter(col("Src_IP") == ip)
            if device_df.rdd.isEmpty():
                continue

            device_data = device_df.select(
                "Idle_Min", "Idle_Max", "Idle_Mean", "Idle_Std",
                "Flow_Duration", "Tot_Fwd_Pkts", "Tot_Bwd_Pkts",
                "TotLen_Fwd_Pkts", "Active_Max", "Active_Min",
                "Src_IP", "Label"
            ).toPandas()

            numeric_features = [
                "Flow_Duration", "Tot_Fwd_Pkts", "Tot_Bwd_Pkts",
                "TotLen_Fwd_Pkts", "Active_Max", "Active_Min",
                "Idle_Min", "Idle_Max", "Idle_Mean", "Idle_Std"
            ]


            #PCA---------------------------------------------------------------------------------------------------------------------------------------------------------------
            device_numeric_data = device_data[numeric_features]
            if device_numeric_data.empty:
                continue

            # Update PCA-CD drift detectors
            device_pca_cd_detectors[ip].update_drift(device_numeric_data)

            change_score = device_pca_cd_detectors[ip].get_latest_change_score()
            if change_score is not None:
                pca_change_score_gauge.labels(src_ip=ip).set(change_score)


            num_pcs = device_pca_cd_detectors[ip].get_num_pcs()
            if num_pcs is not None:
                pca_num_pcs_gauge.labels(src_ip=ip).set(num_pcs)
            # Check for PCA-CD drift events
            drift_events = device_pca_cd_detectors[ip].check_drift(ip)
            print("PCA-CD drift_events")
            print(drift_events)
            for event in drift_events:
                print(f"PCA-CD: Device {ip}: {event} at epoch {epoch_id}")
            #PCA---------------------------------------------------------------------------------------------------------------------------------------------------------------

            #MD3---------------------------------------------------------------------------------------------------------------------------------------------
            # MD3 Detector Update

            if device_data.empty:
                continue

            # Proceed with updating MD3 detector
            device_md3_detector = device_md3_detectors.get(ip)
            if device_md3_detector is None:
                continue  # Detector not initialized for this IP

            # Update MD3 detector with features
            device_md3_detector.update_drift(device_data)

            # Check for drift events
            drift_events = device_md3_detector.check_drift(ip)
            for event in drift_events:
                print(f"MD3: Device {ip}: {event} at epoch {epoch_id}")

            # Update Prometheus metrics
            margin_density = device_md3_detector.get_latest_margin_density()
            if margin_density is not None:
                md3_margin_density_gauge.labels(src_ip=ip).set(margin_density)
            #MD3---------------------------------------------------------------------------------------------------------------------------------------------

        end_time = time.time()
        latency = end_time - start_time
        processing_latency.set(latency)  # Set actual processing latency

        # Log the latency for debugging
        print(f"Batch processed in {latency:.2f} seconds")

    query = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(record_metrics) \
        .trigger(processingTime='1 second') \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()