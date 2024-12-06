import pandas as pd
from sklearn.model_selection import train_test_split

df = pd.read_csv('IoT Network Intrusion Dataset.csv')
# Convert to datetime
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d/%m/%Y %I:%M:%S %p')

df = df.sort_values('Timestamp')

print(f"There are {len(df)} rows, with {df['Src_IP'].nunique()} different source IP and {df['Dst_IP'].nunique()} different destiny IP")

ip_df = df[df['Src_IP'].isin(['192.168.0.13','192.168.0.16','192.168.0.24'])]

ip_df['Date'] = ip_df['Timestamp'].dt.date

ip_df = ip_df.sort_values('Label',ascending=False)

normal_df = ip_df[ip_df['Label'] == 'Normal']
anomaly_df = ip_df[ip_df['Label'] == 'Anomaly']

normal_filtered_df = normal_df.groupby("Src_IP").head(1950)

anomaly_filtered_df = anomaly_df.groupby("Src_IP").head(11700)

filtered_df = pd.concat([normal_filtered_df,anomaly_filtered_df])

filtered_df = filtered_df.sample(frac = 1)

stream_df, model_df = train_test_split(filtered_df, test_size=0.1, stratify=filtered_df['Cat'])
stream_size = len(stream_df)

# Generate the timestamp range for one Src_IP
start_time = "2024-11-23 00:00:00"
timestamps = pd.date_range(start=start_time, periods=stream_size, freq="1S")

# Assign timestamps independently for each Src_IP
stream_df["Custom_Timestamp"] = stream_df.groupby("Src_IP").cumcount().map(
    lambda x: timestamps[x]
)

stream_df.drop(columns=['Date','Timestamp'],inplace=True)
stream_df.rename(columns={"Custom_Timestamp" : "Timestamp"},inplace=True)

stream_df = stream_df.sort_values(by=['Timestamp','Src_IP'])

stream_df.to_csv("processed_data/iot_network_intrusion_dataset_stream.csv",index=False)
model_df.to_csv("processed_data/iot_network_intrusion_dataset_model.csv",index=False)