from t1 import get_pd as gp

import pandas as pd

if __name__ == '__main__':
    #data = gp()
    #data.to_csv('clean_users_churn.csv', index=False)
    data = pd.read_csv('clean_users_churn.csv', parse_dates=['begin_date', 'end_date'])
    print(data.info())

