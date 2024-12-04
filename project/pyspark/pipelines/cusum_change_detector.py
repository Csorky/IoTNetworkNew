from menelaus.change_detection import CUSUM
from prometheus_client import start_http_server, Counter, Gauge

drift_event_counter = Counter('drift_events_total', 'Total number of drift events detected', ['src_ip', 'detector'])
# CUSUM metrics
# Device-specific Drift Detectors
class DeviceDriftDetectors:
    def __init__(self, window_size=10, threshold=5, delta=0.005):
        """
        Initialize drift detectors with required parameters.
        
        :param window_size: Number of recent data points to consider.
        :param threshold: Sensitivity threshold for CUSUM.
        :param delta: Minimum detectable change.
        """
        # Initialize univariate detector for CUSUM
        self.cusum = CUSUM(
            target=None,      # Let CUSUM estimate target from initial data
            sd_hat=None,      # Let CUSUM estimate standard deviation from initial data
            burn_in=window_size,  # Number of initial data points to establish baseline
            delta=delta,      # Sensitivity to changes
            threshold=threshold,  # Threshold for drift detection
            direction=None    # Monitor both increases and decreases
        )

    def update_drift(self, data_univariate, data_multivariate):
        """
        Update drift detectors with new data.
        
        :param data_univariate: Iterable univariate data (list or array).
        :param data_multivariate: Iterable multivariate data (list of tuples or arrays).
        """
        # Update CUSUM with univariate data
        for value in data_univariate:
            self.cusum.update(value)
        # Update other detectors as needed
        # for value in data_univariate:
        #     self.page_hinkley.update(value)
        
        # Update multivariate detectors if implemented
        # self.pcad.update(data_multivariate)
        # self.kdq_tree.update(data_multivariate)

    def check_drift(self, src_ip):
        """
        Check for drift events across all detectors.
        
        :return: List of detected drift events.
        """
        drift_events = []
        if self.cusum.drift_state == 'drift':
            drift_events.append('CUSUM Drift detected')
            drift_event_counter.labels(src_ip=src_ip, detector="CUSUM").inc()  # Increment drift counter
        else:
            print("no drift")
        # Check other detectors as needed
        # if self.page_hinkley.drift_state == 'drift':
        #     drift_events.append('Page Hinkley Drift detected')
        # if self.pcad.drift_state == 'drift':
        #     drift_events.append('PCA-CD Drift detected')
        # if self.kdq_tree.drift_state == 'drift':
        #     drift_events.append('KdqTree Drift detected')
        return drift_events
