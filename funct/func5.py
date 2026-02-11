import pandas as pd
import os

from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from catboost import CatBoostClassifier 
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from category_encoders import CatBoostEncoder
from sqlalchemy import create_engine
from dotenv import load_dotenv 

load_dotenv()

dst_host = os.environ.get('DB_DESTINATION_HOST')
dst_port = os.environ.get('DB_DESTINATION_PORT')            
dst_db = os.environ.get('DB_DESTINATION_NAME')
dst_username = os.environ.get('DB_DESTINATION_USER')
dst_password = os.environ.get('DB_DESTINATION_PASSWORD')
dst_conn = create_engine(f'postgresql://{dst_username}:{dst_password}@{dst_host}:{dst_port}/{dst_db}', connect_args={'sslmode':'require'})



data = pd.read_sql('select * from clean_users_churn', dst_conn, index_col='customer_id')

cat_features = data.select_dtypes(include='object')
potential_binary_features = cat_features.nunique() == 2
binary_cat_features = cat_features[potential_binary_features[potential_binary_features].index]
other_cat_features = cat_features[potential_binary_features[~potential_binary_features].index]
num_features = data.select_dtypes(['float'])

preprocessor = ColumnTransformer(
    [
    ('binary', OneHotEncoder(drop='if_binary'), binary_cat_features.columns.tolist()),
    ('cat', CatBoostEncoder(), other_cat_features.columns.tolist()),
    ('num', StandardScaler(), num_features.columns.tolist())
    ],
    remainder='drop',
    verbose_feature_names_out=False
)

model = CatBoostClassifier(auto_class_weights='Balanced')

pipeline = Pipeline(
    [
        ('preprocessor', preprocessor),
        ('model', model)
    ]
)
pipeline.fit(data, data['target'])

cv_strategy = StratifiedKFold(n_splits=5)
cv_res = cross_validate(
    pipeline,
    data,
    data['target'],
    cv=cv_strategy,
    n_jobs=-1,
    scoring=['f1', 'roc_auc']
    )
for key, value in cv_res.items():
    cv_res[key] = round(value.mean(), 3) 


# params.yaml
# index_col: 'customer_id'
# target_col: 'target'
# one_hot_drop: 'if_binary'
# include: 'object'
# remainder: 'drop'
# verbose_feature_names_out: False
# auto_class_weights: 'Balanced'
# n_splits: 5
# n_jobs: -1
# metrics: ['f1', 'roc_auc']
# Допишите оставшиеся параметры    