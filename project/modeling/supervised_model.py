from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score


class supervisedModel():


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


    def split_data(self):
        # Splits data based on the defined target variables in config
        train_data, test_data = train_test_split(self.data, test_size=0.2, stratify=self.data[self.config['target']], random_state=42)

        self.train_X = train_data.drop(['Label', 'Cat', 'Sub_Cat', 'Timestamp', 'Dst_IP', 'Src_IP', 'Flow_ID'], axis=1)
        self.train_y = train_data[self.config['target']]

        self.test_X = test_data.drop(['Label', 'Cat', 'Sub_Cat', 'Timestamp', 'Dst_IP', 'Src_IP', 'Flow_ID'], axis=1)
        self.test_y = test_data[self.config['target']]
    

    def init_and_train_model(self,):

        if self.config['model'] == 'decision_tree':

            self.model = DecisionTreeClassifier(random_state=42)

        elif self.config['model'] == 'random_forest':

            self.model = RandomForestClassifier(max_depth=5, random_state=42)

        # Add new models if needed

        self.model.fit(self.train_X, self.train_y)


    def predict_val(self,):

        self.pred_y = self.model.predict(self.test_X)


    def get_metrics(self,):

        supported_metrics = {
            'accuracy': accuracy_score,
            'precision': precision_score,
            'recall': recall_score,
            'f1': f1_score,
        }
        
        self.metrics = {}
        for metric in self.config['metrics']:
            if metric not in supported_metrics:
                raise ValueError(f"Metric '{metric}' is not supported.")
            
            if metric == 'precision' or metric == 'recall' or metric == 'f1':
                self.metrics[metric] = supported_metrics[metric](self.test_y, self.pred_y, average='weighted')
            
            else:
                self.metrics[metric] = supported_metrics[metric](self.test_y, self.pred_y)

        return self.metrics



