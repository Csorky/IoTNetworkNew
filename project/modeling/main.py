# main.py
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import os

from fs import correlation, filter_data, shapiro_wilk_test
from load_data import fetch_dataset
from supervised_model import supervisedModel
from unsupervised_model import KMeansModel  # Import the KMeansModel class

def main():

    # Args for fetching dataset
    url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
    token = os.getenv("INFLUXDB_TOKEN", "default_token")
    org = os.getenv("INFLUXDB_ORG", "your_org")
    bucket = os.getenv("INFLUXDB_BUCKET", "iot_data")
    measurement = "kafka_consumer"

    start_time = (datetime.utcnow() - timedelta(hours=6)).isoformat() + "Z"
    batch_size = 30000

    # Fetch the dataset
    data = fetch_dataset(url, token, org, bucket, measurement, start_time, batch_size)
    print("Data shape:", data.shape)

    # Preprocess the data 
    print("Preprocessing data...")
    drop_columns = ["Unnamed: 0", "result", "table", "_start", "_stop", "_time", "_measurement", "host"]
    existing_drop_columns = [col for col in drop_columns if col in data.columns]
    data.drop(existing_drop_columns, axis=1, inplace=True)

    data_numeric = data.select_dtypes([np.number])
    non_numeric_cols = list(set(data.columns) - set(data_numeric.columns))

    # Remove highly correlated features
    correlation(data_numeric, 0.85)
    # Remove columns with just one unique value
    filter_data(data_numeric)

    # Perform Shapiro-Wilk test to further reduce number of features
    data_res = shapiro_wilk_test(data_numeric)

    # Add non-numerical rows
    data_res = data_res.join(data[non_numeric_cols])
    print("Data has been preprocessed.")

    # Modeling step
    print("Building binary classification model...")

    config_bin = {
        'target': 'Label',
        'model': 'decision_tree',
        'metrics': ['accuracy', 'precision', 'recall', 'f1', 'confusion_matrix']
    }
    
    bin_classifier = supervisedModel(data_res, config_bin)
    bin_classifier.split_data()
    bin_classifier.init_and_train_model()
    bin_classifier.predict_val()
    bin_classifier.get_metrics()

    print("Model Metrics:")
    print(bin_classifier.metrics)

    print("Building multiclass classification model...")

    config_multi = {
        'target': 'Cat',
        'model': 'decision_tree',
        'metrics': ['accuracy', 'precision', 'recall', 'f1', 'confusion_matrix']
    }
    
    multi_classifier = supervisedModel(data_res, config_multi)
    multi_classifier.split_data()
    multi_classifier.init_and_train_model()
    multi_classifier.predict_val()
    multi_classifier.get_metrics()

    print("Model Metrics:")
    print(multi_classifier.metrics)

    # KMeans Clustering
    print("\nPerforming KMeans clustering...")

    # Select features for clustering (exclude non-numeric and target columns)
    features_for_clustering = data_res.drop(['Label', 'Cat', 'Sub_Cat', 'Timestamp', 'Dst_IP', 'Src_IP', 'Flow_ID'], axis=1)

    kmeans = KMeansModel(n_clusters=5, max_iter=10000, n_components=2)
    kmeans.fit(features_for_clustering)
    cluster_labels = kmeans.get_labels()

    # Add cluster labels to the preprocessed data
    data_res['KMeans_Cluster'] = cluster_labels
    print("KMeans clustering completed.")

    # Visualize the clusters
    print("Visualizing KMeans clusters...")
    kmeans.visualize_clusters(data_res)
    print("KMeans visualization completed.")

if __name__ == "__main__":
    time.sleep(600)
    main()
