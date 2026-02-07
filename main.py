#from t1 import get_pd as gp
import pandas as pd


if __name__ == '__main__':
    #p = gp()
    p = pd.read_csv('my.csv')
    print(p.head())