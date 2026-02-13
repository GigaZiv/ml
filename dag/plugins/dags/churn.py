import pendulum
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Table, MetaData, Column, Integer, String, DateTime, UniqueConstraint, Float, inspect

with DAG(
    dag_id = "churn_dataset",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
) as dag:
    
    def create_table():
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        users_churn_table = Table(
            'users_churn',
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
            UniqueConstraint('customer_id', name='users_churn_id')
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

        data['target'] = (data['end_date'] != 'No').astype(int)
        data['end_date'].replace({'No': None}, inplace=True)
        #data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)

        ti.xcom_push('transformed_data', data)

    def load(**kwargs):

        ti = kwargs['ti']
        hook = PostgresHook('destination_db')
        data = ti.xcom_pull(task_ids='transform', key='transformed_data')
        hook.insert_rows('users_churn', 
                        replace=True,
                        rows=data.values.tolist(),
                        replace_index=['customer_id'],
                        target_fields=data.columns.tolist())        

        


    create_step = PythonOperator(task_id='create', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)
    extract_step >> transform_step >> load_step