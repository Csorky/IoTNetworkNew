# pyspark/pyspark_streaming.py

from prometheus_client import start_http_server, Counter, Gauge
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import *
import threading
from menelaus.change_detection import CUSUM  # Import only necessary classes
from pipeline_stat.sliding_window import process_device_metrics, sliding_window_aggregation
from pipeline_stat.ema import ExponentialMovingAverage
from pipeline_kdq.kdq_tree import KDQTree

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
flow_duration_gauge = Gauge('flow_duration', 'Flow duration for each source IP', ['src_ip'])
processed_records = Counter('processed_records_total', 'Total number of records processed')
processing_latency = Gauge('processing_latency', 'Latency in processing records')
### ENES METRIC DEFINE START
window_avg_flow_duration_gauge = Gauge('window_avg_flow_duration','Average flow duration for the sliding window')
window_event_count_gauge = Gauge('window_event_count','Number of events in the sliding window')
ema_flow_duration_gauge = Gauge('ema_flow_duration', 'Exponential Moving Average of flow duration for each source IP', ['src_ip'])
device_anomaly_counter = Counter('device_kdq_tree_anomalies','Number of anomalies detected by kdq-Tree for each device',['src_ip'])
### ENES METRIC DEFINE END

# Start Prometheus metrics server
def start_prometheus_server():
    threading.Thread(target=start_http_server, args=(7000,), daemon=True).start()
    print("Prometheus server started on port 7000")

# Device-specific Drift Detectors
class DeviceDriftDetectors:
    def __init__(self, window_size=10, threshold=5, delta=0.005):
        
        # Initialize univariate detector for CUSUM
        self.cusum = CUSUM(
            target=None,      # Let CUSUM estimate target from initial data
            sd_hat=None,      # Let CUSUM estimate standard deviation from initial data
            burn_in=window_size,  # Number of initial data points to establish baseline
            delta=delta,      # Sensitivity to changes
            threshold=threshold,  # Threshold for drift detection
            direction=None    # Monitor both increases and decreases
        )
    

    def update_drift(self, data_univariate, data_multivariate):
        
        for value in data_univariate:
            self.cusum.update(value)
        
    def check_drift(self):
        
        drift_events = []
        if self.cusum.drift_state == 'drift':
            drift_events.append('CUSUM Drift detected')
        else:
            print("no drift")

        return drift_events

def main():
    spark = initialize_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("Flow_ID", StringType(), True),
        StructField("Src_IP", StringType(), True),
        StructField("Src_Port", IntegerType(), True),
        StructField("Flow_Duration", IntegerType(), True), ### Enes import
        StructField("Dst_IP", StringType(), True),
        StructField("Dst_Port", IntegerType(), True),
        StructField("Protocol", StringType(), True),
        StructField("Label", StringType(), True),
        StructField("Cat", StringType(), True),
        StructField("Sub_Cat", StringType(), True),
        StructField("Timestamp", StringType(), True),
        StructField("Idle_Min", FloatType(), True)
    ])

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


    ### ENES EMA VAR START
    global ema_calculator  # Make it accessible in `record_metrics`
    ema_calculator = ExponentialMovingAverage(alpha=0.1)
    ### ENES EMA VAR END

    # Define device IPs (known IPs for each device)
    device_ips = ['192.168.0.13', '192.168.0.24', '192.168.0.16']

    ### ENES VAR DEFINE START
    device_kdq_trees = {ip: KDQTree(threshold=0.1) for ip in device_ips}
    device_kdq_trees = {}
    device_detectors = {ip: DeviceDriftDetectors(window_size=10, threshold=5, delta=0.005) for ip in device_ips}  # window_size=10
    ### ENES VAR DEFINE END

    def record_metrics(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        # Increment processed records
        record_count = batch_df.count()
        processed_records.inc(record_count)
        processing_latency.set(0.5)  # Example latency value


        ### ENES WINDOW START
        sliding_window_df = sliding_window_aggregation(batch_df, window_size="10 minutes", slide_duration="5 minutes")
        windowed_metrics = sliding_window_df.collect()

        for row in windowed_metrics:
            print(f"Window: {row['window']}, Avg Flow Duration: {row['avg_flow_duration']}, Event Count: {row['event_count']}")
            if row['avg_flow_duration'] is not None:  # Update Prometheus metrics
                window_avg_flow_duration_gauge.set(row['avg_flow_duration'])

        window_event_count_gauge.set(row['event_count'])
        ### ENES WINDOW END

        # Update Prometheus metrics using Spark's aggregations
        src_ip_counts = batch_df.groupBy("Src_IP").count().collect()
        for row in src_ip_counts:
            if row["Src_IP"]:
                src_ip_counter.labels(src_ip=row["Src_IP"]).inc(row["count"])

        flow_id_counts = batch_df.groupBy("Flow_ID").count().collect()
        for row in flow_id_counts:
            if row["Flow_ID"]:
                flow_id_counter.labels(flow_id=row["Flow_ID"]).inc(row["count"])

        # Iterate over each device IP and process its data
        for ip in device_ips:
            device_df = batch_df.filter(col("Src_IP") == ip)
            if device_df.rdd.isEmpty():
                continue

            ### Enes KDQ START        
            if ip not in device_kdq_trees: # Ensure the kdq-Tree exists for the device
                print(f"Warning: Device {ip} not found in device_kdq_trees. Initializing a new kdq-Tree.")
                device_kdq_trees[ip] = KDQTree(threshold=0.1)
          
            features_df = device_df.select("Flow_Duration", "Idle_Min", "Src_Port").toPandas().values # Extract features for the kdq-Tree (only the relevant columns for anomaly detection)
            anomalies = device_kdq_trees[ip].detect_anomalies(features_df, k=10)
 
            if anomalies: # Print and log anomalies
                print(f"Anomalies detected for device {ip}: {anomalies}")
                device_anomaly_counter.labels(src_ip=ip).inc(len(anomalies))
            ### Enes KDQ END    

            avg_flow_duration = device_df.agg(F.avg("Flow_Duration").alias("avg_flow_duration")).collect()[0]["avg_flow_duration"]
            if avg_flow_duration is None:
                continue 

            ### Enes EMA START        
            current_ema = ema_calculator.calculate_ema(avg_flow_duration, ip) 
            print(f"Device: {ip}, EMA Flow Duration: {current_ema}") # Print and update Prometheus with the EMA
            ema_flow_duration_gauge.labels(src_ip=ip).set(current_ema)               
            device_data = process_device_metrics(device_df, ip, flow_duration_gauge) # Flow duration

            if device_data.empty:
                continue
            
            print(device_data)

            ### Enes EMA END

            # Extract features
            data_univariate = device_data['Idle_Min'].values.tolist()  # list of 10 Idle_Min values
            print(data_univariate)

            # Update drift detectors
            device_detectors[ip].update_drift(data_univariate, None)  # Pass None for multivariate

            # Check for drift events
            drift_events = device_detectors[ip].check_drift()
            print("drift_events")
            print(drift_events)
            for event in drift_events:
                print(f"Device {ip}: {event} at epoch {epoch_id}")

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
