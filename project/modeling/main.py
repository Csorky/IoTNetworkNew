import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

from fs import correlation, filter_data, shapiro_wilk_test
from load_data import fetch_dataset


def main():

    #args for fetching dataset
    url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token = os.getenv("INFLUXDB_TOKEN", "qenhpUMJyt_fIzacORn4M_0yTUDQqNJByLxwEJPVn0gZlyhcYphnn4zV59gY6og7oT3ASLynkcAjlJOmoE-zMQ==")
    org = os.getenv("INFLUXDB_ORG", "your_org")
    bucket = os.getenv("INFLUXDB_BUCKET", "iot_data")
    measurement = "kafka_consumer"

    start_time = (datetime.utcnow() - timedelta(days=5)).isoformat() + "Z"
    batch_size = 30000

    data = fetch_dataset(url, token, org, bucket, measurement, start_time, batch_size)

    # data = pd.read_csv('/data/iot_network_intrusion_dataset.csv')

    # Preprocess the data
    data.drop(["Unnamed: 0", "result", "table", "_start", "_stop", "_time", "_measurement", "host"], axis=1, inplace=True)

    non_numeric_cols = list(set(data.columns) - set(data.select_dtypes([np.number]).columns))
    #leave only numeric columns and remove highly correlated features
    data_numeric = data.select_dtypes([np.number])
    correlation(data_numeric, 0.85)
    #remove columns with just one unique value
    filter_data(data_numeric)

    #perform shapiro-wilk test to further reduce number of features
    data_res = shapiro_wilk_test(data_numeric)

    #add non-numerical rows
    data_res = data_res.join(data[non_numeric_cols])

    # Modeling step




if __name__ == "__main__":
    main()