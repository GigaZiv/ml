import os
import yaml

import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine



def get_connection():
    load_dotenv()
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')
    
    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}', connect_args={'sslmode':'require'})
    return conn

def get_data():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    # 3.2 — загрузки предыдущих результатов нет, так как это первый шаг
    

    # 3.3 — основная логика
    conn = get_connection()
    data = pd.read_sql('select * from clean_users_churn', conn, index_col=params['index_col'])
    conn.dispose()

    # 3.4 — сохранение результата шага    
    os.makedirs(params['data_dir'], exist_ok=True)
    data.to_csv('data/initial_data.csv', index=False)

if __name__ == '__main__':
    get_data()    
