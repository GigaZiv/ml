def fill_missing_values(data):
    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')
    for col in cols_with_nans:
        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]
        data[col] = data[col].fillna(fill_value)

    return data

def remove_outliers(data):
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
    data = data[~outliers].reset_index(drop=True)

    return data, data[outliers]


    #is_duplicated_id = data.duplicated(subset=['customer_id'], keep=False)
    # параметр keep = False приводит к тому, что и оригинал, и дубликат помечаются как объект с дубликатом
    #print(sum(is_duplicated_id))    
    #['begin_date', 'dependents', 'device_protection', 'end_date', 'gender', 'internet_service', 'monthly_charges', 'multiple_lines', 'online_backup', 'online_security', 'paperless_billing', 'partner', 'payment_method', 'senior_citizen', 'streaming_movies', 'streaming_tv', 'target', 'tech_support', 'total_charges', 'type']

#    feature_cols = data.columns.drop(['id', 'customer_id', 'target']).to_list()
#    is_duplicated_features = pd.DataFrame(data[feature_cols]).duplicated(keep=False).squeeze()
#    print(len(data[is_duplicated_features]))

#    cols_with_nans = data.isnull().sum()
#    cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')
#       if data[col].dtype in [float, int]:
#            fill_value = data[col].median() if not data[col].isna().all() else 0
#        elif data[col].dtype == 'object':
#            fill_value = data[col].mode().iloc[0] if not data[col].mode().empty else 'Unknown'
#        data[col] = data[col].fillna(fill_value)

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

    # print(data[outliers])

import pandas as pd
import sqlalchemy
import os
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
from sklearn.model_selection import StratifiedKFold, cross_validate, train_test_split


# Шаг 1: Получение данных
load_dotenv()
host = os.environ.get('DB_DESTINATION_HOST')
port = os.environ.get('DB_DESTINATION_PORT')
db = os.environ.get('DB_DESTINATION_NAME')
username = os.environ.get('DB_DESTINATION_USER')
password = os.environ.get('DB_DESTINATION_PASSWORD')
dst_conn = create_engine(f'postgresql://{dst_username}:{dst_password}@{dst_host}:{dst_port}/{dst_db}', connect_args={'sslmode':'require'})

data = pd.read_sql('select * from clean_users_churn', dst_conn, index_col='customer_id')

# Шаг 2: Получение названий колонок, их преобразование и обучение модели
cat_features = data.select_dtypes(include='object')
potential_binary_features = cat_features.nunique() == 2
binary_cat_features = cat_features[potential_binary_features[potential_binary_features].index]
other_cat_features = cat_features[potential_binary_features[~potential_binary_features].index]
num_features = data.select_dtypes(['float'])

X_tr, X_val, y_tr, y_val = train_test_split(
    data,
    data['target'],
    stratify=data['target'])

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
pipeline.fit(X_tr, y_tr)

# Шаг 3: Проверка качества на кросс-валидации
cv_strategy = StratifiedKFold(n_splits=5)
cv_res = cross_validate(
    pipeline,
    X_tr,
    y_tr['target'],
    cv=cv_strategy,
    n_jobs=-1,
    scoring=['f1', 'roc_auc']
    )
for key, value in cv_res.items():
    cv_res[key] = round(value.mean(), 3)     