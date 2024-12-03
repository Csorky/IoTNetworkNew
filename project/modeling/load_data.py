import os
import pandas as pd
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
import time
from io import StringIO
import sys


n_fails = 0


def fetch_batch_data(query_api, bucket, measurement, start_time, batch_size=10000):

    if not isinstance(start_time, str):
        start_time = start_time.isoformat() + "Z"
    
    query = f'''
    from(bucket: "{bucket}")
    |> range(start: {start_time})
    |> filter(fn: (r) => r._measurement == "{measurement}")
    |> limit(n: {batch_size})
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    try:

        raw_data = query_api.query_raw(query) # Get raw data

        # transforming into readabale format
        data_str = ''.join(chunk.decode('utf-8') for chunk in raw_data)  # Join generator into a single string

        return pd.read_csv(StringIO(data_str), skiprows=range(3))
    
    except Exception as e:

        print(f"Failed to load the batch: {e}. \n Terminating...")
        # n_fails += 1
        # if n_fails >= 6:
        sys.exit()

# Count query
# query = f'''
# from(bucket: "{bucket}")
#   |> range(start: 0)  // Start from the beginning, or use a specific time range
#   |> filter(fn: (r) => r._measurement == "your_measurement")
#   |> count()
#   '''

def fetch_dataset(url, token, org, bucket, measurement, start_time, batch_size=10000, read_timeout=30_000):

    # Initialize the InfluxDB client
    client = InfluxDBClient(url=url, token=token, org=org, timeout=read_timeout)
    query_api = client.query_api()
    
    batch_num = 0
    all_df = []

    print(f"Fetching data...")

    while True:

        batch_time = time.time()

        try:
            
            batch_df = fetch_batch_data(query_api, bucket, measurement, start_time, batch_size)
            # print(batch_df.shape)

            if batch_df.shape[0] == 1:

                print("All Data has been loaded")
                break

            all_df.append(batch_df)

            start_time = batch_df["_time"].iloc[-1]

            end_time = time.time()
            elapsed_time = end_time - batch_time
            print(f"Batch {batch_num} processed in {round(elapsed_time, 4)} s")

            batch_num += 1

        except Exception as e:

            print(f"Error fetching batch: {e}")
            break
    
    df = pd.concat(all_df, ignore_index=True)
    df = df.drop_duplicates()

    # print(df.head())
    # print(df.shape)
    # # Checking the columns
    # cols = list(df.columns)
    # f = open('modeling/tmp_col_check.txt', 'w')
    # for col in cols:
    #     f.write(col + '\n')

    # Close the client connection
    client.close()


    return df


# def main():
#     #args for fetching dataset
#     url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
#     token = os.getenv("INFLUXDB_TOKEN", "qenhpUMJyt_fIzacORn4M_0yTUDQqNJByLxwEJPVn0gZlyhcYphnn4zV59gY6og7oT3ASLynkcAjlJOmoE-zMQ==")
#     org = os.getenv("INFLUXDB_ORG", "your_org")
#     bucket = os.getenv("INFLUXDB_BUCKET", "iot_data")
#     measurement = "kafka_consumer"

#     start_time = (datetime.utcnow() - timedelta(days=5)).isoformat() + "Z"
#     batch_size = 10000

#     df = fetch_dataset(url, token, org, bucket, measurement, start_time, batch_size)
#     print(df.head())
#     print(df.shape)

    



# if __name__ == "__main__":
#     main()