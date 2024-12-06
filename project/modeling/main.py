import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

from fs import correlation, filter_data, shapiro_wilk_test
from load_data import fetch_dataset
from supervised_model import supervisedModel
from unsupervised_model import DBSCANModel
import time

def main():

    #args for fetching dataset
    # url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    # token = os.getenv("INFLUXDB_TOKEN", "qenhpUMJyt_fIzacORn4M_0yTUDQqNJByLxwEJPVn0gZlyhcYphnn4zV59gY6og7oT3ASLynkcAjlJOmoE-zMQ==")
    # org = os.getenv("INFLUXDB_ORG", "your_org")
    # bucket = os.getenv("INFLUXDB_BUCKET", "iot_data")
    # measurement = "kafka_consumer"

    # start_time = (datetime.utcnow() - timedelta(hours=6)).isoformat() + "Z"
    # batch_size = 30000

    # data = fetch_dataset(url, token, org, bucket, measurement, start_time, batch_size)
    # print("Data shape:", data.shape)

    data = pd.read_csv('../data/iot_network_intrusion_dataset_model.csv')

    # Preprocess the data 
    print("Preprocessing data...")
    #data.drop(["Unnamed: 0", "result", "table", "_start", "_stop", "_time", "_measurement", "host"], axis=1, inplace=True)

    data_numeric = data.select_dtypes([np.number])
    non_numeric_cols = list(set(data.columns) - set(data_numeric.columns))

    # remove highly correlated features
    correlation(data_numeric, 0.85)
    # remove columns with just one unique value
    filter_data(data_numeric)

    #perform shapiro-wilk test to further reduce number of features
    data_res = shapiro_wilk_test(data_numeric)

    #add non-numerical rows
    data_res = data_res.join(data[non_numeric_cols])
    print("Data has been preprocessed.")

    # Modeling step
    print("Building binary classification model...")

    config = {'target' : 'Label',
              'model' : 'decision_tree',
              'metrics' : ['accuracy', 'precision', 'recall', 'f1']}
    
    bin_classifier = supervisedModel(data_res, config)
    bin_classifier.split_data()
    bin_classifier.init_and_train_model()
    bin_classifier.predict_val()
    bin_classifier.get_metrics()

    print(bin_classifier.metrics)

      # DBSCAN Clustering
    print("\nPerforming DBSCAN clustering...")

    
    # Select features for clustering (exclude non-numeric and target columns)
    features_for_clustering = data_res.drop(['Label', 'Cat', 'Sub_Cat', 'Timestamp', 'Dst_IP', 'Src_IP', 'Flow_ID', 'Date'], axis=1)
    print(features_for_clustering.columns)

    dbscan = DBSCANModel(eps=0.75, min_samples=20, metric='euclidean', n_components=2)
    dbscan.fit(features_for_clustering)
    cluster_labels = dbscan.get_labels()

    # Add cluster labels to the original data
    data_res['DBSCAN_Cluster'] = cluster_labels
    print("DBSCAN clustering completed.")

    # Visualize the clusters
    print("Visualizing DBSCAN clusters...")
    dbscan.visualize_clusters(data_res)

    print("DBSCAN visualization completed.")
    
if __name__ == "__main__":
    time.sleep(600)
    main()