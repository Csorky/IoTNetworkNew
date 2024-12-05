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

# class LFRDriftDetector:
#     # Define shared metrics with labels
#     drift_state_gauge = Gauge(
#         "lfr_drift_state",
#         "LFR Drift state (0=no drift, 1=drift)",
#         ["variable_name", "ip"]
#     )
#     drift_event_counter = Counter(
#         "lfr_drift_events",
#         "Number of drift events detected",
#         ["variable_name", "ip"]
#     )
#     tpr_gauge = Gauge(
#         "lfr_true_positive_rate",
#         "True Positive Rate for LFR",
#         ["variable_name", "ip"]
#     )
#     fpr_gauge = Gauge(
#         "lfr_false_positive_rate",
#         "False Positive Rate for LFR",
#         ["variable_name", "ip"]
#     )
#     fnr_gauge = Gauge(
#         "lfr_false_negative_rate",
#         "False Negative Rate for LFR",
#         ["variable_name", "ip"]
#     )
#     tnr_gauge = Gauge(
#         "lfr_true_negative_rate",
#         "True Negative Rate for LFR",
#         ["variable_name", "ip"]
#     )

#     def __init__(self, variable_name, ip, threshold=0.1):
#         """
#         Initialize Linear Four Rates (LFR) drift detector.

#         :param variable_name: Name of the variable being tracked for drift detection.
#         :param ip: IP address associated with this detector instance.
#         :param threshold: Threshold for detecting significant drift in the rates.
#         """
#         self.variable_name = variable_name
#         self.ip = ip
#         self.threshold = threshold
#         self.true_positive_rate = []
#         self.false_positive_rate = []
#         self.false_negative_rate = []
#         self.true_negative_rate = []
#         self.drift_detected = False
#         self.baseline_mean = None
#         self.baseline_std = None
#         self.burn_in_data = []
#         self.burn_in_size = 100  # Number of initial data points for baseline calculation

#     def update(self, data):
#         """
#         Update the detector with new data points.

#         :param data: List of new values for the variable being tracked.
#         """
#         # During burn-in, accumulate data for baseline estimation
#         if len(self.burn_in_data) < self.burn_in_size:
#             self.burn_in_data.extend(data)
#             if len(self.burn_in_data) >= self.burn_in_size:
#                 self._initialize_baseline()
#             return

#         # Generate predictions and ground truth from data
#         predictions, ground_truth = self._generate_predictions(data)

#         # Update Linear Four Rates
#         self._update_rates(predictions, ground_truth)

#         # Update Prometheus metrics
#         self._update_prometheus_metrics()

#     def _initialize_baseline(self):
#         """
#         Compute the baseline mean and standard deviation using burn-in data.
#         """
#         self.baseline_mean = sum(self.burn_in_data) / len(self.burn_in_data)
#         self.baseline_std = (sum((x - self.baseline_mean) ** 2 for x in self.burn_in_data) / len(self.burn_in_data)) ** 0.5
#         print(f"Baseline initialized for {self.variable_name} (IP: {self.ip}): mean={self.baseline_mean}, std={self.baseline_std}")

#     def _generate_predictions(self, data):
#         """
#         Generate predictions and ground truth based on baseline statistics.

#         :param data: List of new values for the variable being tracked.
#         :return: Tuple (predictions, ground_truth)
#         """
#         predictions = [1 if abs(x - self.baseline_mean) > 3 * self.baseline_std else 0 for x in data]
#         ground_truth = [1 if abs(x - self.baseline_mean) > 2 * self.baseline_std else 0 for x in data]
#         return predictions, ground_truth

#     def _update_rates(self, predictions, ground_truth):
#         """
#         Update the Linear Four Rates based on predictions and ground truth.

#         :param predictions: List of binary predictions (1 for drift, 0 for no drift).
#         :param ground_truth: List of binary ground truth values (1 for drift, 0 for no drift).
#         """
#         tp = sum((p == 1 and gt == 1) for p, gt in zip(predictions, ground_truth))
#         fp = sum((p == 1 and gt == 0) for p, gt in zip(predictions, ground_truth))
#         fn = sum((p == 0 and gt == 1) for p, gt in zip(predictions, ground_truth))
#         tn = sum((p == 0 and gt == 0) for p, gt in zip(predictions, ground_truth))

#         total_positive = tp + fn
#         total_negative = fp + tn

#         self.true_positive_rate.append(tp / total_positive if total_positive > 0 else 0)
#         self.false_positive_rate.append(fp / total_negative if total_negative > 0 else 0)
#         self.false_negative_rate.append(fn / total_positive if total_positive > 0 else 0)
#         self.true_negative_rate.append(tn / total_negative if total_negative > 0 else 0)

#         # Check if the drift is detected based on the latest rates
#         self.drift_detected = max(self.false_positive_rate[-1], self.false_negative_rate[-1]) > self.threshold

#         if self.drift_detected:
#             self.drift_event_counter.labels(variable_name=self.variable_name, ip=self.ip).inc()

#     def _update_prometheus_metrics(self):
#         """
#         Update Prometheus metrics for LFR.
#         """
#         if self.true_positive_rate:
#             self.tpr_gauge.labels(variable_name=self.variable_name, ip=self.ip).set(self.true_positive_rate[-1])
#         if self.false_positive_rate:
#             self.fpr_gauge.labels(variable_name=self.variable_name, ip=self.ip).set(self.false_positive_rate[-1])
#         if self.false_negative_rate:
#             self.fnr_gauge.labels(variable_name=self.variable_name, ip=self.ip).set(self.false_negative_rate[-1])
#         if self.true_negative_rate:
#             self.tnr_gauge.labels(variable_name=self.variable_name, ip=self.ip).set(self.true_negative_rate[-1])
        
#         self.drift_state_gauge.labels(variable_name=self.variable_name, ip=self.ip).set(1 if self.drift_detected else 0)

#     def check_drift(self):
#         """
#         Check if drift is detected based on the current rates.

#         :return: Boolean indicating whether drift is detected.
#         """
#         return self.drift_detected


# class LFR_drift:
#     def __init__(self, device_ip):
#         """
#         Initialize the LFR drift detector and Prometheus metrics.
        
#         :param device_ip: IP address of the device associated with this detector.
#         :param window_size: Number of recent data points to consider.
#         """
#         self.lfr = LinearFourRates()
#         self.device_ip = device_ip

#         # Prometheus Gauges to monitor drift metrics
#         # self.drift_state_gauge = Gauge("lfr_drift_state", "Drift state (0=no drift, 1=drift)", ['device_ip'])
#         # self.false_positive_rate_gauge = Gauge("lfr_false_positive_rate", "LFR false positive rate", ['device_ip'])
#         # self.true_positive_rate_gauge = Gauge("lfr_true_positive_rate", "LFR true positive rate", ['device_ip'])

#     def update_drift(self, univariate_data):
#         """
#         Update the LFR drift detector with new univariate data and record metrics.
        
#         :param univariate_data: Iterable univariate data (list or array).
#         """
#         for value in univariate_data:
#             self.lfr.update(value)

#         # Record drift state
#         drift_detected = int(self.lfr.drift_state == 'drift')
#         drift_state_gauge.labels(device_ip=self.device_ip).set(drift_detected)

#         # Record LFR-specific metrics (with fallback for missing attributes)
#         false_positive_rate_gauge.labels(device_ip=self.device_ip).set(getattr(self.lfr, 'false_positive_rate', 0))
#         true_positive_rate_gauge.labels(device_ip=self.device_ip).set(getattr(self.lfr, 'true_positive_rate', 0))

#     def check_drift(self):
#         """
#         Check for drift events in the LFR detector.
        
#         :return: List of detected drift events.
#         """
#         if self.lfr.drift_state == 'drift':
#             return [f'LFR Drift detected for device {self.device_ip}']
#         return []