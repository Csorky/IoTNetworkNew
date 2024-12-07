# pipelines/pca_cd_detector.py

from menelaus.data_drift import PCACD
from prometheus_client import Counter, Gauge

# Define a Counter for PCA-CD drift events
pca_drift_counter = Counter('pca_drift_counter', 'Total number of drift events detected', ['src_ip', 'detector'])

# Define a Gauge for PCA-CD change scores
pca_change_score_gauge = Gauge('pca_change_score', 'Latest change score from PCA-CD', ['src_ip'])

# Define a Gauge for the number of principal components
pca_num_pcs_gauge = Gauge('pca_num_pcs', 'Number of principal components used in PCA-CD', ['src_ip'])


class DevicePCACDDetectors:
    def __init__(self, window_size=10):
        """
        Initialize PCA-CD detectors with required parameters.
        """
        self.pca_cd = PCACD(window_size=window_size, sample_period=0.05, delta=0.001, ev_threshold=0.95)
    def update_drift(self, data_multivariate):
        """
        Update PCA-CD detector with new data.
        :param data_multivariate: Multivariate data as a pandas DataFrame.
        """
        for _, row in data_multivariate.iterrows():
            X = row.values.reshape(1, -1)
            self.pca_cd.update(X)

    def check_drift(self, src_ip):
        """
        Check for drift events.
        :return: List of detected drift events.
        """
        drift_events = []
        if self.pca_cd.drift_state == 'drift':
            print("PCA drift")
            drift_events.append('PCA-CD Drift detected')
            pca_drift_counter.labels(src_ip=src_ip, detector="PCA-CD").inc()  # Increment drift counter
            self.pca_cd.reset()
        else:
            print("no PCA drift")
        return drift_events
    
    def get_latest_change_score(self):
        if self.pca_cd._change_score:
            return self.pca_cd._change_score[-1]
        else:
            return None
        
    def get_num_pcs(self):
        return self.pca_cd.num_pcs