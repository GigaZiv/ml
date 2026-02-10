import pandas as pd

from t1 import get_pd as gp


if __name__ == '__main__':
    data = gp()
    data = pd.read_csv('clean_users_churn.csv')

