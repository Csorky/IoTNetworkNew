from menelaus.change_detection import ADWIN
from prometheus_client import start_http_server, Counter, Gauge

adwin_all_mean = Gauge("adwin_all_mean", "", ['src_ip'])


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
            if detector.drift_state == "drift":
                # Add the mean and adjusted period to the drift event details
                drift_events.append({
                    "value": value,
                    "state": "drift",
                    "mean": detector.mean(),  # Current mean of the monitored stream
                    # "adjusted_period": detector.width  # Width of the ADWIN window
                })

            adwin_all_mean.labels(src_ip=device_ip).set(detector.mean())  # Set mean value


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
            "mean": detector.mean,
            "adjusted_period": detector.width
        }
