# pipelines/md3_detector.py

from menelaus.concept_drift import MD3
from prometheus_client import Counter, Gauge
from sklearn.svm import SVC
import pandas as pd
import numpy as np


# Define a Counter for MD3 drift events
md3_drift_counter = Counter(
    'md3_drift_counter', 'Total number of drift events detected by MD3', ['src_ip', 'detector']
)

# Define a Gauge for MD3 margin density
md3_margin_density_gauge = Gauge(
    'md3_margin_density', 'Latest margin density from MD3', ['src_ip']
)


class DeviceMD3Detectors:
    def __init__(self, target_name='Label', k=3):
        """
        Initialize MD3 detectors with required parameters.
        :param target_name: Name of the target variable in the data.
        :param k: Number of folds for cross-validation.
        """
        self.target_name = target_name
        # Use SGDClassifier for faster training
        self.classifier = SVC(kernel="linear", max_iter=5000)
        self.k = k
        self.md3 = None
        self.feature_columns = ["Flow_Duration", "Tot_Fwd_Pkts", "Tot_Bwd_Pkts",
                "TotLen_Fwd_Pkts", "Active_Max", "Active_Min",
                "Idle_Min", "Idle_Max", "Idle_Mean", "Idle_Std"]

    def initialize_detector(self, initial_training_data_csv, ip_address):
        """
        Initialize the MD3 detector for a specific IP address using initial training data from CSV.
        :param initial_training_data_csv: Path to the CSV file containing initial training data.
        :param ip_address: The IP address to filter the training data.
        """
        # Load the CSV
        data = pd.read_csv(initial_training_data_csv)

        # Filter data by IP
        device_data = data[data['Src_IP'] == ip_address]

        if device_data.empty:
            raise ValueError(f"No training data available for device with IP {ip_address}")

        # Ensure that the target column exists
        if self.target_name not in device_data.columns:
            raise ValueError(f"Target column '{self.target_name}' not found in initial training data.")

        # Build DataFrame with numeric features and target
        initial_training_data_numeric = device_data[self.feature_columns + [self.target_name]].copy()

        # Handle infinite or too large values
        # Replace infinite values with NaN
        initial_training_data_numeric.replace([np.inf, -np.inf], np.nan, inplace=True)
        # Drop rows with NaN values in features or target
        initial_training_data_numeric.dropna(subset=self.feature_columns + [self.target_name], inplace=True)
        if initial_training_data_numeric.empty:
            raise ValueError("No valid initial training data after dropping rows with missing or infinite values.")
        

        X = initial_training_data_numeric[self.feature_columns]
        y = initial_training_data_numeric[self.target_name]
        self.classifier.fit(X, y)

        # Initialize MD3 detector
        self.md3 = MD3(clf=self.classifier, k=self.k)
        print(initial_training_data_numeric)
        self.md3.set_reference(initial_training_data_numeric, target_name=self.target_name)

    def update_drift(self, data):
        if self.md3 is None:
            raise ValueError("MD3 detector has not been initialized. Call 'initialize_detector' first.")

        # Keep only numeric features
        data_numeric = data[self.feature_columns].copy()

        # Handle infinite or NaN values in the new data
        data_numeric.replace([np.inf, -np.inf], np.nan, inplace=True)
        data_numeric.dropna(inplace=True)

        if data_numeric.empty:
            return  # No valid data to update

        # Process data one record at a time
        for _, row in data_numeric.iterrows():
            # Convert the row (Series) to a single-row DataFrame
            single_sample_df = pd.DataFrame([row.values], columns=row.index)
            self.md3.update(single_sample_df)

    def give_oracle_label(self, labeled_sample):
        """
        Provide the detector with a labeled sample to confirm or rule out drift.
        :param labeled_sample: Labeled sample as a pandas DataFrame.
        """
        if self.md3 is None:
            raise ValueError("MD3 detector has not been initialized. Call 'initialize_detector' first.")

        # Keep only numeric features and target
        labeled_sample_numeric = labeled_sample[self.feature_columns + [self.target_name]].copy()

        # Handle infinite or NaN values
        labeled_sample_numeric.replace([np.inf, -np.inf], np.nan, inplace=True)
        labeled_sample_numeric.dropna(inplace=True)
        if labeled_sample_numeric.empty:
            return  # Skip if data is invalid

        self.md3.give_oracle_label(labeled_sample_numeric)

    def check_drift(self, src_ip):
        """
        Check for drift events.
        :return: List of detected drift events.
        """
        drift_events = []
        if self.md3 is None:
            raise ValueError("MD3 detector has not been initialized. Call 'initialize_detector' first.")

        if self.md3.drift_state == 'drift':
            print("MD3 drift detected")
            drift_events.append('MD3 Drift detected')
            md3_drift_counter.labels(src_ip=src_ip, detector="MD3").inc()
            self.md3.reset()
        else:
            print("No MD3 drift")
        return drift_events

    def get_latest_margin_density(self):
        if self.md3 is None:
            return None
        return self.md3.curr_margin_density
