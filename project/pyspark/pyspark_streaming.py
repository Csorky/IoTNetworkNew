# pyspark/pyspark_streaming.py

from prometheus_client import start_http_server, Counter, Gauge
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import threading
import pandas as pd

from pipelines.pagehinley import DevicePageHinkleyDetector
from pipelines.adwin import DeviceADWINDetector
from pipelines.regression import DeviceRegressionDetector


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

# Start Prometheus metrics server
def start_prometheus_server():
    threading.Thread(target=start_http_server, args=(7001,), daemon=True).start()
    print("Prometheus server started on port 7000")

def main():
    spark = initialize_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
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
        StructField("Timestamp", StringType(), True),
        StructField("Idle_Min", FloatType(), True)
    ])

    # Read data from Kafka with maxOffsetsPerTrigger
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "iot_topic") \
        .option("maxOffsetsPerTrigger", 30) \
        .load()

    # Parse the "value" column as JSON and extract the fields using the schema
    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Start Prometheus metrics server
    start_prometheus_server()

    # Define device IPs (known IPs for each device)
    device_ips = ['192.168.0.13', '192.168.0.24', '192.168.0.16']

    # Initialize drift detectors for each device
    device_ph_detectors = DevicePageHinkleyDetector(delta=0.01, threshold=5, burn_in=30)
    device_adwin_detectors = DeviceADWINDetector(delta=0.1)
    streaming_detector = DeviceRegressionDetector()

    # Define the processing function for each batch
    def record_metrics(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

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

            #PH START
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
            #PH END

            # ADWIN detection
            adwin_drift_events = device_adwin_detectors.update_drift(ip, idle_min_values)
            for value in idle_min_values:
                adwin_monitored_value_gauge.labels(src_ip=ip).set(value)

            for event in adwin_drift_events:
                if event['state'] == 'drift':
                    print(f"ADWIN Drift detected for Device {ip}: {event}")
                    adwin_drift_detected_gauge.labels(src_ip=ip).set(1)
                    adwin_mean_gauge.labels(src_ip=ip).set(event["mean"])

            adwin_drift_detected_gauge.labels(src_ip=ip).set(0)
            # ADWIN detection end

            # Update StreamingChangeDetector and compare predictions
            predictions = streaming_detector.update_and_predict(ip, [sum(idle_min_values) / 10])
            streaming_detector.compare_predictions(ip, [sum(idle_min_values) / 10], predictions)


    # Start the streaming query with foreachBatch and trigger settings
    query = df_parsed.writeStream \
        .outputMode("append") \
        .foreachBatch(record_metrics) \
        .trigger(processingTime='1 second') \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()

    # Await termination
    query.awaitTermination()

if __name__ == "__main__":
    main()
