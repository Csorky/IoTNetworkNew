from prometheus_client import Counter, Gauge
from menelaus.change_detection import PageHinkley


class DevicePageHinkleyDetector:
    """
    Handles PageHinkley drift detection for multiple devices.
    """

    # Initialize Prometheus metrics
    ph_drift_detected_counter = Counter('ph_drift_detected_count', '', ['src_ip'])
    ph_monitored_value_gauge = Gauge(
        "ph_monitored_value",
        "Monitored variable value for Page-Hinkley detection",
        ['src_ip']
    )
    ph_drift_detected_gauge = Gauge(
        "ph_drift_detected",
        "Drift detection status for Page-Hinkley (1: drift detected, 0: no drift)",
        ['src_ip']
    )
    ph_drift_window_start_gauge = Gauge(
        "ph_drift_window_start",
        "Start index of detected drift window",
        ['src_ip']
    )
    ph_drift_window_end_gauge = Gauge(
        "ph_drift_window_end",
        "End index of detected drift window",
        ['src_ip']
    )
    ph_drift_duration_gauge = Gauge(
        "ph_drift_duration",
        "Duration of detected drift windows",
        ['src_ip']
    )
    ph_drift_intensity_gauge = Gauge(
        "ph_drift_intensity",
        "Magnitude of deviation during detected drift",
        ['src_ip']
    )
    ph_avg_monitored_value_gauge = Gauge(
        "ph_avg_monitored_value",
        "Rolling average of the monitored variable",
        ['src_ip']
    )
    ph_drift_count_gauge = Gauge(
        "ph_drift_count",
        "Number of drift events detected in the batch",
        ['src_ip']
    )
    ph_drift_actual_value_gauge = Gauge(
        "ph_drift_actual_value",
        "Actual value of a drift",
        ['src_ip']
    )

    def __init__(self, delta=0.01, threshold=10, burn_in=10):
        """
        Initializes a dictionary of PageHinkley detectors for each device.

        Parameters:
            delta (float): Sensitivity parameter for PageHinkley.
            threshold (float): Threshold for detecting drift.
        """
        self.detectors = {}
        self.delta = delta
        self.threshold = threshold
        self.burn_in = burn_in

    def initialize_detector(self, device_ip):
        """
        Initialize a detector for a specific device if not already initialized.
        """
        if device_ip not in self.detectors:
            self.detectors[device_ip] = PageHinkley(
                delta=self.delta,
                threshold=self.threshold,
                burn_in=self.burn_in
            )

    def update_drift(self, device_ip, values):
        """
        Updates the PageHinkley detector for a device with a list of values.

        Parameters:
            device_ip (str): IP address of the device.
            values (list of float): List of data points to update the detector.

        Returns:
            list of dict: Detected drift events for the device.
        """
        if device_ip not in self.detectors:
            self.initialize_detector(device_ip)

        drift_events = []
        detector = self.detectors[device_ip]
        rolling_avg = sum(values) / len(values) if values else 0

        self.ph_avg_monitored_value_gauge.labels(src_ip=device_ip).set(rolling_avg)

        for idx, value in enumerate(values):
            detector.update(X=value)
            self.ph_monitored_value_gauge.labels(src_ip=device_ip).set(value)

            if detector.drift_state == "drift":
                drift_events.append({"value": value, "state": "drift"})

                self.ph_drift_actual_value_gauge.labels(src_ip=device_ip).set(value)

                # Update Prometheus metrics for detected drifts
                self.ph_drift_detected_counter.labels(src_ip=device_ip).inc(1)
                self.ph_drift_detected_gauge.labels(src_ip=device_ip).set(1)

                drift_duration = idx  # Assuming drift starts from index 0
                self.ph_drift_duration_gauge.labels(src_ip=device_ip).set(drift_duration)

                intensity = abs(value - detector._mean)
                self.ph_drift_intensity_gauge.labels(src_ip=device_ip).set(intensity)

                self.ph_drift_window_start_gauge.labels(src_ip=device_ip).set(0)  # Example start index
                self.ph_drift_window_end_gauge.labels(src_ip=device_ip).set(idx)  # Example end index

        # Reset drift status for the next batch
        self.ph_drift_detected_gauge.labels(src_ip=device_ip).set(0)
        self.ph_drift_count_gauge.labels(src_ip=device_ip).set(len(drift_events))

        return drift_events