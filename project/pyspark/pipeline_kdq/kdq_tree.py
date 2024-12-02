import numpy as np
from sklearn.neighbors import KDTree

import numpy as np
from sklearn.neighbors import KDTree

class KDQTree:
    def __init__(self, threshold=0.1):
        """
        Initialize the kdq-Tree for real-time streaming.
        
        :param threshold: Density change threshold for anomaly detection.
        """
        self.tree = None  # The kd-Tree instance
        self.data_points = []  # List to hold all data points
        self.density = {}  # Dictionary to track density for regions
        self.threshold = threshold

    def update_tree(self, new_points):
        """
        Incrementally update the kd-Tree with new data points.
        
        :param new_points: Numpy array of shape (n_samples, n_features).
        """
        self.data_points.extend(new_points)  # Add new points to the dataset
        self.tree = KDTree(np.array(self.data_points))  # Rebuild the kd-Tree

    def detect_anomalies(self, new_points, k=10):
        """
        Detect anomalies for only the new points.
        
        :param new_points: New data points to process.
        :param k: Number of neighbors for density calculation.
        :return: List of anomalies detected for the new points.
        """
        if not self.tree:  # Initialize the tree if it's empty
            self.update_tree(new_points)
            return []

        anomalies = []

        for point in new_points:
            # Query neighbors for the current point
            num_points = len(self.data_points)
            k = min(k, num_points)  # Adjust k to avoid exceeding available points
            dist, ind = self.tree.query([point], k=k)
            
            # Calculate density for the current point
            region_density = len(ind[0]) / (np.pi * np.max(dist)**2)
            key = tuple(point)  # Represent the region as a tuple

            # Detect anomalies by comparing to existing densities
            if key in self.density:
                if abs(region_density - self.density[key]) > self.threshold:
                    anomalies.append((key, region_density))
            else:
                # Treat it as normal if no previous density exists
                pass

            # Update density for this point
            self.density[key] = region_density

        # Incrementally update the tree with the new points
        self.update_tree(new_points)

        return anomalies

