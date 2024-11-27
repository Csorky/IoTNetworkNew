# model/model.py

from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib

def main():
    # Connect to Elasticsearch
    es = Elasticsearch(['http://elasticsearch:9200'])

    # Query to get data from Elasticsearch
    # model.py
    print(es.indices)
    print("-------------------------------")
    res = es.search(
        index="iot_index",
        body={
            "size": 10000,
            "query": {"match_all": {}}
        },
        scroll='2m'
    )

    sid = res['_scroll_id']
    scroll_size = res['hits']['total']['value']

    # Scroll through the data
    data = []
    while scroll_size > 0:
        hits = res['hits']['hits']
        for hit in hits:
            data.append(hit['_source'])
        res = es.scroll(scroll_id=sid, scroll='2m')
        scroll_size = len(res['hits']['hits'])

    # Convert data to DataFrame
    df = pd.DataFrame(data)

    # Preprocess data
    df = df.dropna()
    X = df.drop(['Label', 'Cat', 'Sub_Cat'], axis=1)
    y = df['Label'].apply(lambda x: 1 if x != 'Benign' else 0)

    # Convert categorical data to numerical (if any)
    X = pd.get_dummies(X)

    # Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    # Model Training
    model = RandomForestClassifier()
    model.fit(X_train, y_train)

    # Save the model
    joblib.dump(model, '/app/model.joblib')

    # Print model accuracy
    accuracy = model.score(X_test, y_test)
    print(f"Model Accuracy: {accuracy}")

if __name__ == "__main__":
    main()
