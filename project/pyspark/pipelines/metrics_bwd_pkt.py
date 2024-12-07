from prometheus_client import Gauge, Counter
import numpy as np


class general_metrics:


    metrics = {
        "protocol_anomaly": Gauge(
            "protocol_anomaly", 
            "Protocol-based anomaly score", 
            ["variable_name", "protocol", "ip"]),
        "correlation_flow_bytes_pkts": Gauge(
            "correlation_flow_bytes_pkts", 
            "Correlation between Flow_Byts/s and Flow_Pkts/s", 
            ["variable_name", "ip"]),
        "idle_time_stats": Gauge(
            "idle_time_stats", 
            "Idle time statistics", 
            ["ip"]),
    }


    def __init__(self, variable_name, ip, window_size=50):

        self.variable_name = variable_name

        self.ip = ip

        self.window_size = window_size

        self.data_window = []
        

    def update(self, data):
        """
        Update the metrics with new data points.
        
        :param data: List of new data points.
        :param timestamp: Current timestamp of the batch.
        :param protocol: Protocol in use (e.g., TCP, UDP).
        :param flow_bytes: Flow bytes per second data.
        :param flow_pkts: Flow packets per second data.
        :param idle_times: Idle time data.
                                             protocol = bwd_df['Protocol'], 
                                     bwd_pkt_len_max = bwd_df['Bwd_Pkt_Len_Max'],
                                     flow_pkts = bwd_df['Flow_Pkts/s'],
                                     idle_times = bwd_df['Idle_Min']
        """
        self.data_window.extend(data)
        if len(self.data_window) > self.window_size:
            self.data_window = self.data_window[-self.window_size:]

        if len(self.data_window) < self.window_size:
            return  # Not enough data for analysis
        
        protocols = data['Protocol'].unique()

        for protocol in protocols:
            
            protocol_data = data[data['Protocol'] == protocol]['Bwd_Pkt_Len_Max'].values
            self._record_protocol_anomaly(protocol, protocol_data)


        self._record_correlation_flow_metrics(data['Bwd_Pkt_Len_Max'], data['Flow_Pkts/s'])

        self._record_idle_time_stats(data['Idle_Min'])


    def _record_protocol_anomaly(self, protocol, data):
        """
        Records a protocol-based anomaly score.

        :param protocol: Protocol in use (e.g., 0, 6, 17).
        :param data: List or array of numerical data related to the protocol.
        """
        if len(data) == 0:
            return  # No data to process

        # Map protocol numbers to names for better clarity
        protocol_map = {0: "HOPOPT", 6: "TCP", 17: "UDP"}
        protocol_name = protocol_map.get(protocol, "Unknown")

        if protocol == 6:  # TCP
            anomaly_score = np.std(data)
        elif protocol == 17:  # UDP
            anomaly_score = np.mean(data) + 2 * np.std(data)
        elif protocol == 0:  # HOPOPT or Reserved
            anomaly_score = np.max(data) - np.min(data)
        else:
            anomaly_score = 0

        # Record the anomaly score
        self.metrics["protocol_anomaly"].labels(
            variable_name=self.variable_name,
            protocol=protocol_name,
            ip=self.ip
        ).set(anomaly_score)

    def _record_correlation_flow_metrics(self, bwd_pkt_len_max, flow_pkts):

        correlation = np.corrcoef(bwd_pkt_len_max, flow_pkts)[0, 1] if len(bwd_pkt_len_max) > 1 and len(flow_pkts) > 1 else 0
        self.metrics["correlation_flow_bytes_pkts"].labels(variable_name=self.variable_name, ip=self.ip).set(correlation)


    def _record_idle_time_stats(self, idle_times):

        idle_mean = np.mean(idle_times)
        self.metrics["idle_time_stats"].labels(ip=self.ip).set(idle_mean)
