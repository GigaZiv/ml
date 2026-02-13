from t1 import get_pd_source, create_connection_destionation, get_users_churn, get_clean_users_churn


import pandas as pd
import numpy as np
from pandas import DataFrame


def get_source():
    data = get_pd_source()
    conn = create_connection_destionation()
    
    data['end_date'] = data['end_date'].replace({'No': None})
    data['target'] = data['end_date'].notna().astype(int)       
   

    print(data.head())
        
    data.to_sql('users_churn', conn, index=False, if_exists='replace', )


def get_analiz():
    data = get_users_churn()
    conn = create_connection_destionation()

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

    num_cols = data.select_dtypes(['float']).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold * IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)

    outliers = potential_outliers.any(axis=1)
    
    data = data[outliers].reset_index(drop=True)  
    

    print(data.head())
        
    data.to_sql('users_churn', conn, index=False, if_exists='replace', )

def get_fit():
    import yaml
    import os
    import joblib
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from category_encoders import CatBoostEncoder
    from sklearn.preprocessing import StandardScaler, OneHotEncoder
    from catboost import CatBoostClassifier

    with open('F:\\Django\\projects\\ML\\ml\\funct\\yaml\\params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    data = get_clean_users_churn()
    #data.to_csv('clean_users_churn.csv', index=False)
    #data = pd.read_csv('clean_users_churn.csv', parse_dates=['begin_date', 'end_date'])

    print(data.head())

    cat_features = data.select_dtypes(include='object')
    potential_binary_features = cat_features.nunique() == 2

    binary_cat_features = cat_features[potential_binary_features[potential_binary_features].index]
    other_cat_features = cat_features[potential_binary_features[~potential_binary_features].index]
    num_features = data.select_dtypes(['float'])

    preprocessor = ColumnTransformer(
        [
            ('binary', OneHotEncoder(drop=params['one_hot_drop']), binary_cat_features.columns.tolist()),
            ('cat', CatBoostEncoder(return_df=False), other_cat_features.columns.tolist()),
            ('num', StandardScaler(), num_features.columns.tolist())
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = CatBoostClassifier(auto_class_weights=params['auto_class_weights'])

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )
    pipeline.fit(data, data[params['target_col']])

    os.makedirs('models', exist_ok=True)
    joblib.dump(pipeline, 'models/fitted_model.pkl')

if __name__ == '__main__':
    #get_source()
    get_analiz()
    #get_fit()









