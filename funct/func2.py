import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from category_encoders import CatBoostEncoder

data = pd.read_csv('clean_users_churn.csv', parse_dates=['begin_date', 'end_date'])

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from catboost import CatBoostClassifier
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from category_encoders import CatBoostEncoder

X_tr, X_val, y_tr, y_val = train_test_split(data, data['target'], stratify=data['target'])

preprocessor = ColumnTransformer(
    [
    ('binary', OneHotEncoder(drop='if_binary'), binary_cols),
    ('cat', CatBoostEncoder(), non_binary_cat_cols),
    ('num', StandardScaler(), num_cols)
    ],
    remainder='drop',
    verbose_feature_names_out=False
)
model = CatBoostClassifier(auto_class_weights='Balanced')

# создайте пайплайн
pipeline = Pipeline(
    # ваш код здесь #
    [
    ("preprocessor", preprocessor),
    ("model", model)
    ]
)
    


# обучите пайплайн
# ваш код здесь #
pipeline.fit(X_tr, y_tr)

# получите предсказания для тестовой выборки
# ваш код здесь #
y_pred = pipeline.predict(X_val)
y_pred_proba = pipeline.predict_proba(X_val)[:, 1]

print('f1:', f1_score(y_val, y_pred))
print('roc_auc:', roc_auc_score(y_val, y_pred_proba))