import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import shapiro

import warnings
warnings.filterwarnings('ignore')


def correlation(dataset, threshold):
    col_corr = set() # Set of all the names of deleted columns
    corr_matrix = dataset.corr().abs()
    for i in range(len(corr_matrix.columns)):
        for j in range(i):
            if (corr_matrix.iloc[i, j] >= threshold) and (corr_matrix.columns[j] not in col_corr):
                colname = corr_matrix.columns[i] # get the name of column
                col_corr.add(colname)
                if colname in dataset.columns:
                    del dataset[colname] # delete the column from the dataset



def filter_data(dataset):
    #remove features containing just one value
    std = dataset.describe().loc['std']

    zero_std = []
    for i in std.index:
        if std[i] == 0:
            # print(i)
            zero_std.append(i)

    dataset.drop(zero_std, axis=1, inplace=True)

def shapiro_wilk_test(dataset, threshold=0.4):
    data_numeric_new = dataset[np.isfinite(dataset).all(1)]
    shapiro_wilk = data_numeric_new.apply(lambda x: shapiro(x).statistic)

    return data_numeric_new[shapiro_wilk[shapiro_wilk >= threshold].index]

# def main():


#     # Preprocess the data
#     data = pd.read_csv('/data/iot_network_intrusion_dataset.csv')
#     non_numeric_cols = list(set(data.columns) - set(data.select_dtypes([np.number]).columns))
#     #leave only numeric columns and remove highly correlated features
#     data_numeric = data.select_dtypes([np.number])
#     correlation(data_numeric, 0.85)
#     #remove columns with just one unique value
#     filter_data(data_numeric)

#     #perform shapiro-wilk test to further reduce number of features
#     data_res = shapiro_wilk_test(data_numeric)

#     #add non-numerical rows
#     data_res = data_res.join(data[non_numeric_cols])



#     # Modeling step




# if __name__ == "__main__":
#     main()