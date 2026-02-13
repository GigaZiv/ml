import pandas as pd
import numpy as np

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime, UniqueConstraint, Float, inspect

def create_table():
    hook = PostgresHook('destination_db')
    db_conn = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    users_churn_table = Table(
        'clean_users_churn',
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('customer_id', String),
        Column('begin_date', DateTime),
        Column('end_date', DateTime),
        Column('type', String),
        Column('paperless_billing', String),
        Column('payment_method', String),
        Column('monthly_charges', Float),
        Column('total_charges', Float),
        Column('internet_service', String),
        Column('online_security', String),
        Column('online_backup', String),
        Column('device_protection', String),
        Column('tech_support', String),
        Column('streaming_tv', String),
        Column('streaming_movies', String),
        Column('gender', String),
        Column('senior_citizen', Integer),
        Column('partner', String),
        Column('dependents', String),
        Column('multiple_lines', String),
        Column('target', Integer),
        #UniqueConstraint('customer_id', name='id')
    )
    if not inspect(db_conn).has_table(users_churn_table.name): 
        metadata.create_all(db_conn)

def extract(**kwargs):
    hook = PostgresHook('source_db')
    conn = hook.get_conn()
    ti = kwargs['ti']
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
    conn.close()
    ti.xcom_push('extracted_data', data)

def transform(**kwargs):
    ti = kwargs['ti']

    data = ti.xcom_pull(task_ids='extract', key='extracted_data')

    feature_cols = data.columns.drop('customer_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)

    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')

    for col in cols_with_nans:

        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]

        data[col] = data[col].fillna(fill_value)
        
    # num_cols = data.select_dtypes(['float']).columns
    # threshold = 1.5
    # potential_outliers = pd.DataFrame()

    # for col in num_cols:
    #     Q1 = data[col].quantile(0.25)
    #     Q3 = data[col].quantile(0.75)
    #     IQR = Q3 - Q1
    #     margin = threshold * IQR
    #     lower = Q1 - margin
    #     upper = Q3 + margin
    #     potential_outliers[col] = ~data[col].between(lower, upper)

    # outliers = potential_outliers.any(axis=1)
    
    # data = data[~outliers].reset_index(drop=True)

    ti.xcom_push('transformed_data', data)

def load(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)
    hook.insert_rows(
        table="clean_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['customer_id'],
        rows=data.values.tolist()
)
