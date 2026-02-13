from sqlalchemy import create_engine
import os
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

def create_connection_destionation():
    
    host = os.environ.get('DB_DESTINATION_HOST')
    port = os.environ.get('DB_DESTINATION_PORT')
    db = os.environ.get('DB_DESTINATION_NAME')
    username = os.environ.get('DB_DESTINATION_USER')
    password = os.environ.get('DB_DESTINATION_PASSWORD')

    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')

    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    return conn

def create_connection_source():
    
    host = os.environ.get('DB_SOURCE_HOST')
    port = os.environ.get('DB_SOURCE_PORT')
    db = os.environ.get('DB_SOURCE_NAME')
    username = os.environ.get('DB_SOURCE_USER')
    password = os.environ.get('DB_SOURCE_PASSWORD')

    print(f'postgresql://{username}:{password}@{host}:{port}/{db}')

    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{db}')
    return conn

def get_clean_users_churn():     
    conn = create_connection_destionation()    
    data = pd.read_sql('select * from clean_users_churn', conn, parse_dates=['begin_date', 'end_date'])
    conn.dispose()
    return data

def get_users_churn():     
    conn = create_connection_destionation()
    data = pd.read_sql('select * from users_churn', conn, parse_dates=['start_date', 'end_date'])    
    conn.dispose()
    return data

def get_pd_source():     
    conn = create_connection_source()

    sql = f"""
            select
                c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
                i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
                p.gender, p.senior_citizen, p.partner, p.dependents,
                ph.multiple_lines
            from contracts as c
            left join internet as i on i.customer_id = c.customer_id
            left join personal as p on p.customer_id = c.customer_id
            left join phone as ph on ph.customer_id = c.customer_id

        """
    data = pd.read_sql(sql, conn, parse_dates=['begin_date', 'end_date'])
    return data

