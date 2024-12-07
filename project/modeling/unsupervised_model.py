# kmeans_model.py

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import plotly.express as px

class KMeansModel:
    def __init__(self, n_clusters=5, max_iter=300, random_state=42, n_components=2):
        """
        Initialize the KMeansModel.

        Parameters:
        - n_clusters: The number of clusters to form.
        - max_iter: Maximum number of iterations of the k-means algorithm for a single run.
        - random_state: Determines random number generation for centroid initialization.
        - n_components: Number of components for PCA dimensionality reduction for visualization.
        """
        self.n_clusters = n_clusters
        self.max_iter = max_iter
        self.random_state = random_state
        self.n_components = n_components
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=self.n_components, random_state=self.random_state)
        self.kmeans = KMeans(
            n_clusters=self.n_clusters,
            max_iter=self.max_iter,
            random_state=self.random_state,
            n_init='auto'  # Updated to 'auto' for scikit-learn 1.4+
        )
        self.labels = None
        self.data_pca = None

    def fit(self, data):
        """
        Fit the KMeans model to the data.

        Parameters:
        - data: pandas DataFrame containing the features to cluster.
        """
        # Standardize the data
        scaled_data = self.scaler.fit_transform(data)
        
        # Apply KMeans
        self.kmeans.fit(scaled_data)
        self.labels = self.kmeans.labels_
        
        # For visualization, reduce to 2 dimensions
        self.data_pca = self.pca.fit_transform(scaled_data)

    def get_labels(self):
        """
        Get the cluster labels.

        Returns:
        - labels: numpy array of cluster labels.
        """
        return self.labels

    def visualize_clusters(self, original_data, label_column='Cat', timestamp_column='Timestamp'):
        """
        Visualize the clusters using Plotly.

        Parameters:
        - original_data: pandas DataFrame containing the original data.
        - label_column: Name of the column containing the true labels.
        - timestamp_column: Name of the column containing timestamps.
        """
        if self.data_pca is None or self.labels is None:
            raise ValueError("Model has not been fitted yet.")

        # Create a DataFrame for plotting
        plot_df = pd.DataFrame({
            'PC1': self.data_pca[:, 0],
            'PC2': self.data_pca[:, 1],
            'Cluster': self.labels,
            label_column: original_data[label_column],
            timestamp_column: original_data[timestamp_column]
        })

        # Convert clusters to string for better plotting
        plot_df['Cluster'] = plot_df['Cluster'].astype(str)

        # Create the scatter plot
        fig = px.scatter(
            plot_df,
            x='PC1',
            y='PC2',
            color='Cluster',
            symbol=label_column,
            hover_data=[timestamp_column],
            title='KMeans Clustering Results',
            width=800,
            height=600
        )

        fig.update_layout(legend_title_text='Clusters')
        fig.show()



    