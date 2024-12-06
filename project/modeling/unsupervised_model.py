from sklearn.cluster import DBSCAN


class unsupervisedModel():


    def __init__(self, data, config):
        """
        Initialize supervised model class

        Parameters:

        data: preprocessed dataset
        config: configuration dictionary. Included keys: target, model, metrics
        """
        super().__init__()

        self.data = data
        self.config = config

    def preprocess_unsupervised(self):
        
        self.categorical_data = self.data[['Label', 'Cat', 'Sub_Cat', 'Timestamp', 'Dst_IP', 'Src_IP', 'Flow_ID']]
        self.data = self.data.drop(['Label', 'Cat', 'Sub_Cat', 'Timestamp', 'Dst_IP', 'Src_IP', 'Flow_ID'], axis=1)

    def init_and_train_model(self):

        #
        self.model = DBSCAN().fit(self.data)

    