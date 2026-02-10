from sqlalchemy import create_engine
import os
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

def create_connection():
    
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')

    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')

    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    return conn

def get_pd(): 
    #ewfwefwe   
    conn = create_connection()
    data = pd.read_sql('select * from users_churn', conn, parse_dates=['start_date', 'end_date'])
    return data