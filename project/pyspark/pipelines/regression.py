import pandas as pd
import lightgbm as lgb


class DeviceRegressionDetector:
    """
    Handles streaming-based LightGBM predictions for multiple devices using lagged features.
    """

    def __init__(self, lags=5, threshold=20):
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

        Parameters:
            device_ip (str): IP address of the device.
        """
        if device_ip not in self.models:
            self.models[device_ip] = lgb.LGBMRegressor()
            self.lag_values[device_ip] = [None] * self.lags

    def update_and_predict(self, device_ip, values):
        """
        Update the lag values for a device with new streaming data and make predictions.

        Parameters:
            device_ip (str): IP address of the device.
            values (list of float): List of new 'Idle_Min' values.

        Returns:
            list of float: List of predictions for the new data.
        """
        if device_ip not in self.models:
            self.initialize_device(device_ip)

        predictions = []
        for new_value in values:
            lag_values = self.lag_values[device_ip]

            # Skip prediction if lag values are not initialized properly
            if None in lag_values:
                lag_values.pop(0)
                lag_values.append(new_value)
                continue

            # Predict using the current lag values
            input_features = [lag_values]
            prediction = self.models[device_ip].predict(input_features)[0]
            predictions.append(prediction)

            # Update lag values
            lag_values.pop(0)
            lag_values.append(new_value)

        return predictions

    def compare_predictions(self, device_ip, actual_values, predicted_values):
        """
        Compare actual and predicted values to detect significant differences.

        Parameters:
            device_ip (str): IP address of the device.
            actual_values (list of float): List of actual 'Idle_Min' values.
            predicted_values (list of float): List of predicted values.

        Returns:
            None
        """
        for actual, predicted in zip(actual_values, predicted_values):
            difference = abs(actual - predicted)
            print(f"Device {device_ip}: Actual={actual}, Predicted={predicted}, Difference={difference}")
            if difference > self.threshold:
                print(f"Alert: Significant prediction difference for {device_ip}: {difference}")
