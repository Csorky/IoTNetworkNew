# pyspark/pyspark_streaming.py

from prometheus_client import start_http_server, Counter, Gauge
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import threading
import pandas as pd

from menelaus.change_detection import CUSUM  # Import only necessary classes
from menelaus.change_detection import PageHinkley  # Import only necessary classes
from menelaus.change_detection import ADWIN  # Import only necessary classes



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

batch_sum = Gauge('batch_sum', '',['src_ip'])
batch_mean = Gauge('batch_mean', '',['src_ip'])
batch_value = Gauge("batc_value","",['src_ip'])
adwin_mean = Gauge("adwin_mean", "",['src_ip'])

# Start Prometheus metrics server
def start_prometheus_server():
    threading.Thread(target=start_http_server, args=(7001,), daemon=True).start()
    print("Prometheus server started on port 7000")

# Device-specific Drift Detectors
class DeviceDriftDetectors:
    def __init__(self, window_size=10, threshold=5, delta=0.005):
        """
        Initialize drift detectors with required parameters.
        
        :param window_size: Number of recent data points to consider.
        :param threshold: Sensitivity threshold for CUSUM.
        :param delta: Minimum detectable change.
        """
        # Initialize univariate detector for CUSUM
        self.cusum = CUSUM(
            target=None,      # Let CUSUM estimate target from initial data
            sd_hat=None,      # Let CUSUM estimate standard deviation from initial data
            burn_in=window_size,  # Number of initial data points to establish baseline
            delta=delta,      # Sensitivity to changes
            threshold=threshold,  # Threshold for drift detection
            direction=None    # Monitor both increases and decreases
        )
        # Initialize other detectors as needed
        self.page_hinkley = PageHinkley(delta=0.01, threshold=15, burn_in=30)

        self.adwin = ADWIN()
        # self.pcad = PCACD(window_size=100)
        # self.kdq_tree = KdqTreeStreaming(parameters)

    def update_drift(self, data_univariate, data_multivariate):
        """
        Update drift detectors with new data.
        
        :param data_univariate: Iterable univariate data (list or array).
        :param data_multivariate: Iterable multivariate data (list of tuples or arrays).
        """
        # Update CUSUM with univariate data
        for value in data_univariate:
            # print("CUSUM Update value", value)
            self.cusum.update(value)
   
        # Update other detectors as needed
        for value in data_univariate:
            # print("PH Update value", value)
            self.page_hinkley.update(value)
        
        for value in data_univariate:
            self.adwin.update(value)
        # Update multivariate detectors if implemented
        # self.pcad.update(data_multivariate)
        # self.kdq_tree.update(data_multivariate)

    def check_drift(self):
        """
        Check for drift events across all detectors.
        
        :return: List of detected drift events.
        """
        drift_events = []
        if self.cusum.drift_state == 'drift':
            drift_events.append('CUSUM Drift detected')
        else:
            print("No CUSUM")
        # Check other detectors as needed
        if self.page_hinkley.drift_state == 'drift':
            drift_events.append('Page Hinkley Drift detected')
        else:
            print("No PH")

        rec_list = []
        if self.adwin.drift_state == 'drift':
            drift_events.append('ADWIN Drift detected')
            retrain_start = self.adwin.retraining_recs[0]
            retrain_end = self.adwin.retraining_recs[1]
            rec_list.append([retrain_start, retrain_end])
        else:
            print("No ADWIN")

        return drift_events, rec_list, self.adwin.mean()

def main():
    spark = initialize_spark()
    # spark.sparkContext.setLogLevel("WARN")
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
    device_detectors = {ip: DeviceDriftDetectors(window_size=10, threshold=5, delta=0.005) for ip in device_ips}  # window_size=10

    # Define the processing function for each batch
    def record_metrics(batch_df, epoch_id):
        if batch_df.rdd.isEmpty():
            return

        # Increment processed records
        record_count = batch_df.count()
        processed_records.inc(record_count)
        processing_latency.set(0.5)  # Example latency value

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

            # Collect the data for the device
            device_data = device_df.select("Idle_Min", "Src_IP").toPandas()

            if device_data.empty:
                continue
            
            print(device_data)
            # Extract features
            data_univariate = device_data['Idle_Min'].values.tolist()  # list of 10 Idle_Min values

            batch_sum.labels(src_ip=ip).set(sum(data_univariate))
            batch_mean.labels(src_ip=ip).set(sum(data_univariate) / 10)

            for v in data_univariate:
                batch_value.labels(src_ip = ip).set(v)

            # Update drift detectors
            device_detectors[ip].update_drift(data_univariate, None)  # Pass None for multivariate

            # Check for drift events
            drift_events, rec_list, adw = device_detectors[ip].check_drift()

            adwin_mean.labels(src_ip = ip).set(adw)

            print("drift_events")
            print(drift_events)
            print(rec_list)
            print(adw)
            print()
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
