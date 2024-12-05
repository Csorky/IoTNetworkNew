from menelaus.concept_drift import LinearFourRates
from prometheus_client import Gauge, Counter



class LFRDriftDetector:
    # Define shared metrics with labels
    metrics = {
        "drift_state": Gauge(
            "lfr_drift_state",
            "LFR Drift state (0=no drift, 1=drift)",
            ["variable_name", "ip"],
        ),
        "drift_events": Counter(
            "lfr_drift_events",
            "Number of drift events detected",
            ["variable_name", "ip"],
        ),
        "true_positive_rate": Gauge(
            "lfr_true_positive_rate",
            "True Positive Rate for LFR",
            ["variable_name", "ip"],
        ),
        "false_positive_rate": Gauge(
            "lfr_false_positive_rate",
            "False Positive Rate for LFR",
            ["variable_name", "ip"],
        ),
        "false_negative_rate": Gauge(
            "lfr_false_negative_rate",
            "False Negative Rate for LFR",
            ["variable_name", "ip"],
        ),
        "true_negative_rate": Gauge(
            "lfr_true_negative_rate",
            "True Negative Rate for LFR",
            ["variable_name", "ip"],
        ),
    }

    def __init__(self, variable_name, ip, threshold=0.1, window_size=50):
        """
        Initialize the Linear Four Rates (LFR) drift detector.

        :param variable_name: Name of the variable to monitor.
        :param ip: Associated IP address.
        :param threshold: Threshold for detecting significant drift.
        :param window_size: Size of the rolling window for dynamic baselines.
        """
        self.variable_name = variable_name
        self.ip = ip
        self.threshold = threshold
        self.window_size = window_size

        self.data_window = []
        self.baseline_mean = None
        self.baseline_std = None
        self.drift_detected = False

        self.rates = {
            "tp": 0,
            "fp": 0,
            "fn": 0,
            "tn": 0,
        }

    def update(self, data):
        """
        Update the detector with new data points.

        :param data: List of new data points.
        """
        # Add data to the rolling window
        self.data_window.extend(data)
        if len(self.data_window) > self.window_size:
            self.data_window = self.data_window[-self.window_size:]

        if len(self.data_window) < self.window_size:
            return  # Not enough data for analysis

        # Update baseline statistics
        self._update_baseline()

        # Generate predictions and ground truth
        predictions, ground_truth = self._generate_predictions(data)

        # Update rates and check for drift
        self._update_rates(predictions, ground_truth)
        self._update_prometheus_metrics()

    def _update_baseline(self):
        """
        Update the baseline mean and standard deviation using the rolling window.
        """
        self.baseline_mean = sum(self.data_window) / len(self.data_window)
        self.baseline_std = (
            sum((x - self.baseline_mean) ** 2 for x in self.data_window) / len(self.data_window)
        ) ** 0.5

    def _generate_predictions(self, data):
        """
        Generate predictions and ground truth.

        :param data: List of new data points.
        :return: Tuple (predictions, ground_truth)
        """
        predictions = [1 if abs(x - self.baseline_mean) > 3 * self.baseline_std else 0 for x in data]
        ground_truth = [1 if abs(x - self.baseline_mean) > 2 * self.baseline_std else 0 for x in data]
        return predictions, ground_truth

    def _update_rates(self, predictions, ground_truth):
        """
        Update Linear Four Rates based on predictions and ground truth.

        :param predictions: List of binary predictions.
        :param ground_truth: List of binary ground truth values.
        """
        tp = sum(p == 1 and gt == 1 for p, gt in zip(predictions, ground_truth))
        fp = sum(p == 1 and gt == 0 for p, gt in zip(predictions, ground_truth))
        fn = sum(p == 0 and gt == 1 for p, gt in zip(predictions, ground_truth))
        tn = sum(p == 0 and gt == 0 for p, gt in zip(predictions, ground_truth))

        total_positive = tp + fn
        total_negative = fp + tn

        self.rates["tp"] = tp / total_positive if total_positive else 0
        self.rates["fp"] = fp / total_negative if total_negative else 0
        self.rates["fn"] = fn / total_positive if total_positive else 0
        self.rates["tn"] = tn / total_negative if total_negative else 0

        self.drift_detected = max(self.rates["fp"], self.rates["fn"]) > self.threshold
        if self.drift_detected:
            self.metrics["drift_events"].labels(variable_name=self.variable_name, ip=self.ip).inc()

    def _update_prometheus_metrics(self):
        """
        Update Prometheus metrics for LFR.
        """
        self.metrics["true_positive_rate"].labels(variable_name=self.variable_name, ip=self.ip).set(self.rates["tp"])
        self.metrics["false_positive_rate"].labels(variable_name=self.variable_name, ip=self.ip).set(self.rates["fp"])
        self.metrics["false_negative_rate"].labels(variable_name=self.variable_name, ip=self.ip).set(self.rates["fn"])
        self.metrics["true_negative_rate"].labels(variable_name=self.variable_name, ip=self.ip).set(self.rates["tn"])
        self.metrics["drift_state"].labels(variable_name=self.variable_name, ip=self.ip).set(int(self.drift_detected))