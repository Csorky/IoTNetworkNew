# from menelaus.detector import PageHinkley
from menelaus.change_detection import PageHinkley  # Import only necessary classes


class DevicePageHinkleyDetector:
    """
    Handles PageHinkley drift detection for multiple devices.
    """

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

        for value in values:
            detector.update(X=value)
            if detector.drift_state == "drift":
                drift_events.append({"value": value, "state": "drift"})


        return drift_events

    def check_drift(self, device_ip):
        """
        Placeholder for advanced drift state analysis.

        Parameters:
            device_ip (str): IP address of the device.

        Returns:
            list: Detected drift events (currently based on `update_drift` logic).
        """

        # Additional logic can be implemented here if required
        return []
