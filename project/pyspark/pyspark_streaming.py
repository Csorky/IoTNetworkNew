# pyspark/pyspark_streaming.py
import os
from prometheus_client import start_http_server, Counter, Gauge
from pipelines.pca_cd_detector import *
from pipelines.md3_detector import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import threading
from pipelines.pagehinley import DevicePageHinkleyDetector
from pipelines.adwin import DeviceADWINDetector
from pipelines.regression import DeviceRegressionDetector
import time
import statistics
import pandas as pd
import numpy as np
from pipelines.cusum_change_detector import CusumDriftDetector
from pipelines.ddm_drift_detection import DeviceDDMDetector
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

# Page-Hinkley metrics
ph_drift_detected_counter = Counter('ph_drift_detected_count', '', ['src_ip'])
ph_monitored_value_gauge = Gauge(
    "ph_monitored_value",
    "Monitored variable value for Page-Hinkley detection",
    ['src_ip']
)
ph_drift_detected_gauge = Gauge(
    "ph_drift_detected",
    "Drift detection status for Page-Hinkley (1: drift detected, 0: no drift)",
    ['src_ip']
)
ph_drift_window_start_gauge = Gauge(
    "ph_drift_window_start",
    "Start index of detected drift window",
    ['src_ip']
)
ph_drift_window_end_gauge = Gauge(
    "ph_drift_window_end",
    "End index of detected drift window",
    ['src_ip']
)

# ADWIN metrics
adwin_monitored_value_gauge = Gauge("adwin_monitored_value", "Monitored variable value for ADWIN detection", ['src_ip'])
adwin_drift_detected_gauge = Gauge("adwin_drift_detected", "Drift detection status for ADWIN (1: drift detected, 0: no drift)", ['src_ip'])
adwin_mean_gauge = Gauge("adwin_mean", "Mean value for ADWIN detection during drift", ['src_ip'])

# Prometheus metrics for regression
prediction_difference_counter = Counter(
    "prediction_difference_alerts", 
    "Number of alerts due to significant prediction differences", 
    ['src_ip']
)

prediction_difference_gauge = Gauge(
    "prediction_difference_value", 
    "Magnitude of prediction difference", 
    ['src_ip']
)

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
        .option("maxOffsetsPerTrigger", 120) \
        .load()

    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    start_prometheus_server()

    device_ips = ['192.168.0.13', '192.168.0.24', '192.168.0.16']
    device_cusum_detectors = {ip: CusumDriftDetector(window_size=10, threshold=15, delta=0.005) for ip in device_ips}
    device_ddm_detectors = {ip: DeviceDDMDetector() for ip in device_ips}
    
     # Initialize drift detectors for each device
    device_ph_detectors = DevicePageHinkleyDetector(delta=0.01, threshold=5, burn_in=30)
    device_adwin_detectors = DeviceADWINDetector(delta=0.1)
    streaming_detector = DeviceRegressionDetector()
    
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



    training_data = pd.read_csv(training_data_path)

    
    #DDM
    # Train initial models
    for ip in device_ips:
        device_data_ddm = training_data[training_data['Src_IP'] == ip]
        if not device_data_ddm.empty:
            # ADD MORE FEATURES 
            X_train = device_data_ddm[["Flow_Duration", "Tot_Fwd_Pkts", "Tot_Bwd_Pkts"]].values
            y_train = device_data_ddm["Label"].values
            device_ddm_detectors[ip].fit_initial_model(X_train, y_train)
            print(f"Initial model trained for device {ip}")
        else:
            print(f"No initial data available for device {ip}.")


    # Dictionaries for tracking per-device global min and max (STATS)
    global global_flow_duration_min, global_flow_duration_max
    global_flow_duration_min = {ip: float("inf") for ip in device_ips}
    global_flow_duration_max = {ip: float("-inf") for ip in device_ips}

    global  evaluation_metrics
    evaluation_metrics = {ip: {"accuracy": [], "detected_drifts": 0, "detected_warnings": 0} for ip in device_ips}

    # buffer for training data
    # device_training_buffers = {ip: {'X': [], 'y': []} for ip in device_ips}

    # Initialize global dictionaries to track metrics
    evaluation_metrics = {ip: {"accuracy": [], "detected_drifts": 0, "detected_warnings": 0} for ip in device_ips}

    def report_metrics():
        for ip, metrics in evaluation_metrics.items():
            avg_accuracy = np.mean(metrics["accuracy"]) if metrics["accuracy"] else 0
            print(f"Device {ip}:")
            print(f"  - Average Accuracy: {avg_accuracy:.2f}")
            print(f"  - Detected Drifts: {metrics['detected_drifts']}")
            print(f"  - Detected Warnings: {metrics['detected_warnings']}")


    def record_metrics(batch_df, epoch_id):
        global global_flow_duration_min
        global global_flow_duration_max
        global evaluation_metrics

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


            # Collect the data for the device
            device_data = device_df.select("Flow_Duration", "Src_IP").toPandas()
            if device_data.empty:
                continue
            print(device_data)

            # Flow_duration gauge
            data_univariate = device_data['Flow_Duration'].values.tolist() 
            print(data_univariate)
            # for visualization
            flow_duration_gauge.labels(src_ip=ip).set(statistics.mean(data_univariate))
            for value in data_univariate:
                flow_duration_gauge.labels(src_ip=ip).set(value) 
            
            # Update Prometheus (micro-batch) metrics for each device
            flow_duration_gauge.labels(src_ip=f"{ip}_min").set(device_agg_metrics["Flow_Duration_Min"])
            flow_duration_gauge.labels(src_ip=f"{ip}_max").set(device_agg_metrics["Flow_Duration_Max"])
            flow_duration_gauge.labels(src_ip=f"{ip}_mean").set(device_agg_metrics["Flow_Duration_Mean"])
            
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
                
            # Collect the data for the device
            device_data = device_df.select("Idle_Min", "Src_IP").toPandas()
            if device_data.empty:
                continue

            # Update PageHinkley drift detectors
            idle_min_values = device_data['Idle_Min'].values.tolist()

            #PH START------------------------------------------------------------------------------------------------------------------------------------------------------
            drift_events = device_ph_detectors.update_drift(ip, idle_min_values)

            # Update Prometheus metrics for the monitored variable
            for idx, value in enumerate(idle_min_values):
                ph_monitored_value_gauge.labels(src_ip=ip).set(value)

            # Track drift events and update Prometheus
            for event in drift_events:
                print(f"PageHinkley Drift detected for Device {ip}: {event}")
                ph_drift_detected_gauge.labels(src_ip=ip).set(1)  # Mark drift detected
                ph_drift_detected_counter.labels(src_ip=ip).inc(1)  # Mark drift detected
                ph_drift_window_start_gauge.labels(src_ip=ip).set(idx)  # Start of drift window
                ph_drift_window_end_gauge.labels(src_ip=ip).set(idx)    # End of drift window

            # Reset drift detection status for the next batch
            ph_drift_detected_gauge.labels(src_ip=ip).set(0)
            #PH END----------------------------------------------------------------------------------------------------------------------------------------------------------------

            # ADWIN detection----------------------------------------------------------------------------------------------------------------------------------------------------------------
            adwin_drift_events = device_adwin_detectors.update_drift(ip, idle_min_values)
            for value in idle_min_values:
                adwin_monitored_value_gauge.labels(src_ip=ip).set(value)

            for event in adwin_drift_events:
                if event['state'] == 'drift':
                    print(f"ADWIN Drift detected for Device {ip}: {event}")
                    adwin_drift_detected_gauge.labels(src_ip=ip).set(1)
                    adwin_mean_gauge.labels(src_ip=ip).set(event["mean"])

            adwin_drift_detected_gauge.labels(src_ip=ip).set(0)
            # ADWIN detection end----------------------------------------------------------------------------------------------------------------------------------------------------------------

            # Update StreamingChangeDetector and compare predictions----------------------------------------------------------------------------------------------------------------------------------------------------------------
            predictions = streaming_detector.update_and_predict(ip, [sum(idle_min_values) / 10])
            streaming_detector.compare_predictions(ip, [sum(idle_min_values) / 10], predictions)
            #----------------------------------------------------------------------------------------------------------------------------------------------------------------
            device_data = device_df.select("Idle_Min", "Src_IP","Flow_Duration", "Tot_Fwd_Pkts", "Tot_Bwd_Pkts", "Label").toPandas()
            if device_data.empty:
                continue
            print(device_data)

            # CUSUM record metrics----------------------------------------------------------------------------------------------------------------------------------------------------------------
            data_univariate = device_data['Idle_Min'].values.tolist() 
            print(data_univariate)
            # for cusum visualization
            idle_min_mean_gauge.labels(src_ip=ip).set(statistics.mean(data_univariate))
            for value in data_univariate:
                idle_min_gauge.labels(src_ip=ip).set(value) 
            
            # Update CUSUM drift detectors
            device_cusum_detectors[ip].update_drift(data_univariate, None)  # Pass None for multivariate

            # Check for cusum drift events
            drift_events = device_cusum_detectors[ip].check_drift(ip)
            print("drift_events")
            print(drift_events)
            for event in drift_events:
                print(f"CUSUM: Device {ip}: {event} at epoch {epoch_id}")
            #----------------------------------------------------------------------------------------------------------------------------------------------------------------
           # Make predictions and update DDM----------------------------------------------------------------------------------------------------------------------------------------------------------------
            X_test = device_data[["Flow_Duration", "Tot_Fwd_Pkts", "Tot_Bwd_Pkts"]].values
            y_true = device_data["Label"].values
            correct_predictions = 0

            for i in range(len(X_test)):
                y_pred = device_ddm_detectors[ip].classifier.predict([X_test[i]])
                drift_state = device_ddm_detectors[ip].ddm.update(y_true[i], y_pred[0])

                # Track prediction accuracy
                if y_pred[0] == y_true[i]:
                    correct_predictions += 1


                if drift_state == "drift":
                    evaluation_metrics[ip]["detected_drifts"] += 1
                    print(f"DDM Drift detected for Device {ip} at epoch {epoch_id}")
                    # Optionally retrain model with recent data
                    device_ddm_detectors[ip].retrain_model()
                elif drift_state == "warning":
                    evaluation_metrics[ip]["detected_warnings"] += 1
                    print(f"DDM Warning for Device {ip} at epoch {epoch_id}")
                
            batch_accuracy = correct_predictions / len(X_test)
            evaluation_metrics[ip]["accuracy"].append(batch_accuracy)
            print(f"Device {ip} - Batch Accuracy: {batch_accuracy:.2f}")

             # Log accuracy to Prometheus
            flow_duration_gauge.labels(src_ip=f"{ip}_accuracy").set(batch_accuracy)
            #----------------------------------------------------------------------------------------------------------------------------------------------------------------


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

        report_metrics()
        
    query = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(record_metrics) \
        .trigger(processingTime='1 second') \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()