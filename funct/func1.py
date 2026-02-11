import pandas as pd

data = pd.read_csv('clean_users_churn.csv', parse_dates=['begin_date', 'end_date'])

from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split

X_tr, X_val, y_tr, y_val = train_test_split(
    data,
    data['target'],
    stratify=data['target']
)

# Тренировочная выборка
cat_features_tr = X_tr.select_dtypes(include='object')
potential_binary_features_tr = cat_features_tr.nunique() == 2

binary_cat_features_tr = cat_features_tr[potential_binary_features_tr[potential_binary_features_tr].index]
other_cat_features_tr = cat_features_tr[potential_binary_features_tr[~potential_binary_features_tr].index]
num_features_tr = X_tr.select_dtypes(['float'])

# Валидационная выборка
cat_features_val = X_val.select_dtypes(include='object')
potential_binary_features_val = cat_features_val.nunique() == 2

binary_cat_features_val = cat_features_val[potential_binary_features_val[potential_binary_features_val].index]
other_cat_features_val = cat_features_val[potential_binary_features_val[~potential_binary_features_val].index]
num_features_val = X_val.select_dtypes(['float'])

binary_cols = binary_cat_features_tr.columns.tolist()
non_binary_cat_cols = other_cat_features_tr.columns.tolist()
num_cols = num_features_tr.columns.tolist()

# определите список трансформаций в рамках ColumnTransformer
preprocessor = ColumnTransformer(
    transformers=[
        ('num', 'passthrough', num_cols),
        ('binary_cat', 'passthrough', binary_cols),
        ('other_cat', 'passthrough', non_binary_cat_cols)
    ],
    verbose_feature_names_out=False
)

# трансформируйте исходные данные data с помощью созданного preprocessor
X_tr_transformed = preprocessor.fit_transform(X_tr)
X_val_transformed = preprocessor.transform(X_val)

print(X_val_transformed)
print(pd.DataFrame(X_val_transformed, columns=preprocessor.get_feature_names_out()))