from menelaus.concept_drift import DDM
from sklearn.naive_bayes import GaussianNB
from prometheus_client import Counter

# DDM metrics
warning_event_counter = Counter('warning_events_total', 'Total number of warning events detected', ['src_ip', 'detector'])
ddm_event_counter = Counter('ddm_events_total', 'Total number of drift events detected', ['src_ip', 'detector'])


class DeviceDDMDetector:
    def __init__(self, n_threshold=100, warning_scale=7, drift_scale=10):
        self.ddm = DDM(n_threshold=n_threshold, warning_scale=warning_scale, drift_scale=drift_scale)
        self.classifier = GaussianNB()
        self.trained = False

    def fit_initial_model(self, X_train, y_train):
        self.classifier.fit(X_train, y_train)
        self.trained = True

    def detect_and_retrain(self, X_test, y_true, src_ip):
        if not self.trained:
            raise ValueError("Model not trained. Call fit_initial_model first.")
        
        y_pred = self.classifier.predict(X_test)
        self.ddm.update(y_true, y_pred)

        if self.ddm.drift_state == 'drift':
            ddm_event_counter.labels(src_ip=src_ip, detector="DDM").inc()
            return "drift"
        elif self.ddm.drift_state == 'warning':
            warning_event_counter.labels(src_ip=src_ip, detector="DDM").inc()
            return "warning"
        return "stable"
