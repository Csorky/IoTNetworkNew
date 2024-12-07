import pandas as pd
import lightgbm as lgb
from prometheus_client import Counter, Gauge
import numpy as np

class DeviceRegressionDetector:
    """
    Handles streaming-based LightGBM predictions for multiple devices using lagged features.
    """

    def __init__(self, lags=5, threshold=100):
        """
        Initializes a dictionary of LightGBM models and lag values for each device.

        Parameters:
            lags (int): Number of lagged features to use for predictions.
            threshold (float): Difference threshold for raising alerts.
        """
        self.csv_path = '/data/iot_network_intrusion_dataset_model.csv'
        self.lags = lags
        self.threshold = threshold
        self.models = {}  # Dictionary to store models for each device
        self.lag_values = {}  # Dictionary to store lag values for each device
        self._prepare_initial_data()

        # Prometheus metrics
        self.mae_gauge = Gauge(
            "device_mean_absolute_error", 
            "Mean Absolute Error for predictions", 
            ['src_ip']
        )
        self.rmse_gauge = Gauge(
            "device_root_mean_squared_error", 
            "Root Mean Squared Error for predictions", 
            ['src_ip']
        )
        self.alert_count = Counter(
            "device_prediction_alert_count", 
            "Count of alerts for significant prediction differences", 
            ['src_ip']
        )
        self.error_rate_gauge = Gauge(
            "device_error_rate", 
            "Proportion of predictions exceeding threshold", 
            ['src_ip']
        )
        self.predicted_values_gauge = Gauge(
            "device_predicted_value", 
            "Last predicted value for each device", 
            ['src_ip']
        )
        self.actual_values_gauge = Gauge(
            "device_actual_value", 
            "Last actual value for each device", 
            ['src_ip']
        )
    def _prepare_initial_data(self):
        """
        Prepare the initial dataset for training models for each device.
        """ 
        df = pd.read_csv(self.csv_path)
        df['CC'] = df.groupby('Src_IP').cumcount()
        df['RANK'] = df['CC'] // 10
        ranked_df = df.groupby(['RANK', 'Src_IP'])['Idle_Min'].mean().reset_index()

        # Train a model for each device
        for device_ip in ranked_df['Src_IP'].unique():
            device_data = ranked_df[ranked_df['Src_IP'] == device_ip]
            for lag in range(1, self.lags + 1):
                device_data[f'Idle_Min_lag_{lag}'] = device_data['Idle_Min'].shift(lag)

            train_data = device_data.dropna()
            X_train = train_data[[f'Idle_Min_lag_{lag}' for lag in range(1, self.lags + 1)]]
            y_train = train_data['Idle_Min']

            # Train a LightGBM model for the device
            model = lgb.LGBMRegressor()
            model.fit(X_train, y_train)
            self.models[device_ip] = model

            # Initialize lag values for the device
            self.lag_values[device_ip] = [
                device_data[f'Idle_Min_lag_{i + 1}'].tail(1).values[0]
                for i in range(self.lags)
            ]

    def initialize_device(self, device_ip):
        """
        Initialize a device model and lag values if not already initialized.
        """
        if device_ip not in self.models:
            self.models[device_ip] = lgb.LGBMRegressor()
            self.lag_values[device_ip] = [None] * self.lags
            self.data_buffer[device_ip] = []

    def update_and_predict(self, device_ip, values):
        """
        Update lag values and predict in batches for a device.
        """
        if device_ip not in self.models:
            self.initialize_device(device_ip)

        predictions = []
        lag_values = self.lag_values[device_ip]

        for new_value in values:
            lag_values.pop(0)
            lag_values.append(new_value)

            # Skip if lag values are incomplete
            if None in lag_values:
                continue

            # Predict in batch
            prediction = self.models[device_ip].predict([lag_values])[0]
            self.predicted_values_gauge.labels(src_ip=device_ip).set(prediction)  # Log predicted value
            predictions.append(prediction)

        return predictions

    def compare_predictions(self, device_ip, actual_values, predicted_values):
        """
        Compare actual and predicted values, log metrics, and retrain if needed.
        """
        differences = [abs(a - p) for a, p in zip(actual_values, predicted_values)]
        mae = np.mean(differences)
        rmse = np.sqrt(np.mean([d**2 for d in differences]))
        error_rate = sum(1 for diff in differences if diff > self.threshold) / len(differences)

        # Log metrics to Prometheus
        self.mae_gauge.labels(src_ip=device_ip).set(mae)
        self.rmse_gauge.labels(src_ip=device_ip).set(rmse)
        self.error_rate_gauge.labels(src_ip=device_ip).set(error_rate)

        # Check for alerts and retrain if difference exceeds threshold
        for actual, predicted, difference in zip(actual_values, predicted_values, differences):
            self.actual_values_gauge.labels(src_ip=device_ip).set(actual)  # Log actual value
            if difference > self.threshold:
                print(f"Device {device_ip}: Actual={actual}, Predicted={predicted}, Difference={difference}")
                self.alert_count.labels(src_ip=device_ip).inc()
                print(f"Alert: Significant prediction difference for {device_ip}: {difference}")
