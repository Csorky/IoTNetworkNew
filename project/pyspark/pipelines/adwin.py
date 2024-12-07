from menelaus.change_detection import ADWIN
from prometheus_client import Gauge, Counter

# Prometheus metrics
adwin_mean_value_gauge = Gauge("adwin_mean_value", "Monitored variable mean value for ADWIN detection", ['src_ip'])
adwin_monitored_value_gauge = Gauge("adwin_monitored_value", "Monitored variable value for ADWIN detection", ['src_ip'])
adwin_drift_count = Counter("adwin_drift_count", "Count of drifts detected by ADWIN", ['src_ip'])
adwin_data_points_processed = Counter("adwin_data_points_processed", "Number of data points processed per device", ['src_ip'])
adwin_last_drift_value = Gauge("adwin_last_drift_value","",['src_ip'])
adwin_samples_since_reset = Gauge("adwin_samples_since_reset","",['src_ip'])
adwin_retraining_start = Gauge("adwin_retraining_start","",['src_ip'])
adwin_retraining_end = Gauge("adwin_retraining_end","",['src_ip'])




class DeviceADWINDetector:
    """
    Handles ADWIN drift detection for multiple devices.
    """

    def __init__(self, delta=0.01):
        """
        Initializes a dictionary of ADWIN detectors for each device.

        Parameters:
            delta (float): Confidence parameter for ADWIN.
        """
        self.detectors = {}
        self.delta = delta

    def initialize_detector(self, device_ip: str):
        """
        Initialize a detector for a specific device if not already initialized.
        """
        if device_ip not in self.detectors:
            self.detectors[device_ip] = ADWIN(delta=self.delta)

    def update_drift(self, device_ip: str, values):
        """
        Updates the ADWIN detector for a device with a list of values.

        Parameters:
            device_ip (str): IP address of the device.
            values (list of float): List of data points to update the detector.

        Returns:
            list of dict: Detected drift events for the device, including mean value and adjusted period.
        """
        if device_ip not in self.detectors:
            self.initialize_detector(device_ip)

        drift_events = []
        detector = self.detectors[device_ip]

        for value in values:
            detector.update(X=value)

            # Increment the number of data points processed
            adwin_data_points_processed.labels(src_ip=device_ip).inc()

            # Update monitored value and mean for Prometheus
            adwin_monitored_value_gauge.labels(src_ip=device_ip).set(value)
            adwin_mean_value_gauge.labels(src_ip=device_ip).set(detector.mean())

            if detector.drift_state == "drift":
                drift_events.append({
                    "value": value,
                    "state": "drift",
                    "mean": detector.mean()
                })
                adwin_last_drift_value.labels(src_ip=device_ip).set(value)
                adwin_samples_since_reset.labels(src_ip=device_ip).set(detector.samples_since_reset)
                adwin_retraining_start.labels(src_ip=device_ip).set(detector.retraining_recs[0])
                adwin_retraining_end.labels(src_ip=device_ip).set(detector.retraining_recs[1])


                # Increment drift count
                adwin_drift_count.labels(src_ip=device_ip).inc()

        return drift_events

    def check_drift(self, device_ip: str):
        """
        Advanced drift state analysis for a specific device.

        Parameters:
            device_ip (str): IP address of the device.

        Returns:
            dict: Current drift state, mean value, and adjusted period for the device.
        """
        if device_ip not in self.detectors:
            self.initialize_detector(device_ip)

        detector = self.detectors[device_ip]
        return {
            "drift_state": detector.drift_state,
            "mean": detector.mean()
        }
