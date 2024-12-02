# sliding_window.py
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from prometheus_client import start_http_server, Counter, Gauge
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
import threading

def sliding_window_aggregation(df: DataFrame, window_size: str = "10 minutes", slide_duration: str = "5 minutes"):
    return df.withColumn("event_time", F.to_timestamp(F.col("Timestamp"))) \
        .groupBy(F.window(F.col("event_time"), window_size, slide_duration)) \
        .agg(
            F.avg("Flow_Duration").alias("avg_flow_duration"),
            F.stddev("Flow_Duration").alias("stddev_flow_duration"),
            F.count("*").alias("event_count")
        )

def process_device_metrics(device_df, ip, flow_duration_gauge):
    """
    Process Flow_Duration and Idle_Min metrics for a specific device IP.
    
    Args:
    - device_df (DataFrame): Filtered DataFrame for the specific device.
    - ip (str): Device IP being processed.
    - flow_duration_gauge (Gauge): Prometheus Gauge to update average flow duration.
    
    Returns:
    - device_data (DataFrame): Processed data as a Pandas DataFrame or None if empty.
    """
    if device_df.rdd.isEmpty():
        return None

    # Extract Flow_Duration for the specific Src_IP
    flow_duration_data = device_df.select("Flow_Duration").collect()
    if flow_duration_data:
        # Calculate total and average flow duration for the batch
        total_flow_duration = sum(row["Flow_Duration"] for row in flow_duration_data)
        avg_flow_duration = total_flow_duration / len(flow_duration_data)
        flow_duration_gauge.labels(src_ip=ip).set(avg_flow_duration)

    # Collect the data for the device
    device_data = device_df.select("Idle_Min", "Src_IP").toPandas()
    if device_data.empty:
        return None

    return device_data

