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