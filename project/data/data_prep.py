import pandas as pd
import datetime

#read the src file
df = pd.read_csv('IoT Network Intrusion Dataset.csv')
# Convert to datetime
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%d/%m/%Y %I:%M:%S %p')
#sort the values
df = df.sort_values('Timestamp')

print(f"There are {len(df)} rows, with {df['Src_IP'].nunique()} different source IP and {df['Dst_IP'].nunique()} different destiny IP")

print(f"{round(df['Src_IP'].value_counts(normalize=True).to_frame().head(3).sum().values[0],2)}% of the data comes from these 3 IP sources:")
print(df['Src_IP'].value_counts(normalize=True).to_frame().head(3).index[0])
print(df['Src_IP'].value_counts(normalize=True).to_frame().head(3).index[1])
print(df['Src_IP'].value_counts(normalize=True).to_frame().head(3).index[2])
print("so we will analyse these 3.")
#75% of our data are come from these 3 IP source


ip_df = df[df['Src_IP'].isin(['192.168.0.13','192.168.0.16','192.168.0.24'])]
print(f"Sorted data len is", len(ip_df))


ip_df['Date'] = ip_df['Timestamp'].dt.date


sample_df = ip_df[ip_df['Date'] == datetime.date(2019,9,10)]

filtered_df = sample_df.groupby("Src_IP").head(10800)

# Generate the date range
start_time = "2024-11-23 00:00:00"
timestamps = pd.date_range(start=start_time, periods=10800, freq="1S")

# Assign timestamps independently for each Src_IP
filtered_df["Custom_Timestamp"] = filtered_df.groupby("Src_IP").cumcount().map(
    lambda x: timestamps[x]
)

filtered_df.drop(columns=['Date','Timestamp'],inplace=True)
filtered_df.rename(columns={"Custom_Timestamp" : "Timestamp"},inplace=True)

filtered_df = filtered_df.sort_values(by=['Timestamp','Src_IP'])

filtered_df.to_csv("processed_data/iot_network_intrusion_dataset.csv",index=False)
