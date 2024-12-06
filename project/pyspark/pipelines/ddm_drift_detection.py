from menelaus.concept_drift import DDM
from sklearn.naive_bayes import GaussianNB
from prometheus_client import Counter
from sklearn.preprocessing import LabelEncoder
import numpy as np


# DDM metrics
warning_event_counter = Counter('warning_events_total', 'Total number of warning events detected', ['src_ip', 'detector'])
ddm_event_counter = Counter('ddm_events_total', 'Total number of drift events detected', ['src_ip', 'detector'])

class DeviceDDMDetector:
    def __init__(self, n_threshold=100, warning_scale=2, drift_scale=3, retrain_window_size=100):
        """
        Initialize the DDM detector and classifier.

       Args:
            n_threshold (int, optional): the minimum number of samples required
                to test whether drift has occurred. Defaults to 30.
            warning_scale (int, optional): defines the threshold over which to
                enter the warning state. Defaults to 2.
            drift_scale (int, optional): defines the threshold over which to
                enter the drift state. Defaults to 3."""
        
        self.ddm = DDM(n_threshold=n_threshold, warning_scale=warning_scale, drift_scale=drift_scale, )
        self.classifier = GaussianNB()
        self.trained = False
        self.retrain_window_size = retrain_window_size
        self.retraining_buffer = {"X": [], "y": []}  # Buffer for recent data
        self.label_encoder = LabelEncoder() 

    def fit_initial_model(self, X_train, y_train):
        """
        Fit the initial model using training data.

        Args:
        X_train (ndarray): Feature matrix for training.
        y_train (ndarray): Labels for training.
        """
        y_train_encoded = self.label_encoder.fit_transform(y_train)
        self.classifier.fit(X_train, y_train)
        self.trained = True


    def detect_and_retrain(self, X_test, y_true, src_ip):
        """
        Detect drift and retrain if necessary.

        Args:
            X_test (ndarray): Feature matrix for prediction.
            y_true (ndarray): True labels for the test data.
            src_ip (str): Source IP address (device identifier).

        Returns:
            str: The drift state ('stable', 'warning', or 'drift').
        """
        if not self.trained:
            raise ValueError("Model not trained. Call fit_initial_model first.")

        # Initialize drift state
        drift_state = "stable"

        for i in range(len(X_test)):
            single_x_test = X_test[i].reshape(1, -1)  # Single sample
            single_y_true = y_true[i:i+1]             # Single label
            single_y_true_encoded = self.label_encoder.transform(single_y_true)
            single_y_pred = self.classifier.predict(single_x_test)

            # Update DDM for the single observation
            self.ddm.update(single_y_true_encoded[0], single_y_pred[0])

            # Add data to retraining buffer
            self.retraining_buffer["X"].append(single_x_test.flatten())
            self.retraining_buffer["y"].append(single_y_true[0])

            # Check drift state and handle it
            if self.ddm.drift_state == "drift":
                ddm_event_counter.labels(src_ip=src_ip, detector="DDM").inc()
                print(f"Drift detected for {src_ip}. Retraining model.")
                self._retrain_model()
                drift_state = "drift"
                break  # Exit early since drift has been detected
            elif self.ddm.drift_state == "warning" and drift_state != "drift":
                warning_event_counter.labels(src_ip=src_ip, detector="DDM").inc()
                drift_state = "warning"

        print(f"[{src_ip}] Final DDM State: {drift_state}")
        return drift_state

    def _retrain_model(self):
        """
        Retrain the model using recent data from the buffer.
        """
        if len(self.retraining_buffer["X"]) >= self.retrain_window_size:
            X_retrain = np.array(self.retraining_buffer["X"][-self.retrain_window_size:])
            y_retrain = np.array(self.retraining_buffer["y"][-self.retrain_window_size:])
            y_retrain_encoded = self.label_encoder.transform(y_retrain)
            self.classifier.fit(X_retrain, y_retrain_encoded)
            print("Model retrained successfully.")
        else:
            print("Not enough data in retraining buffer to retrain.")
