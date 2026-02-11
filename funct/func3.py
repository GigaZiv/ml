from sklearn.model_selection import StratifiedKFold, cross_validate

cv_strategy = StratifiedKFold(n_splits=5)

#cv_res = cross_validate(X_tr, y_tr, scoring=['f1', 'roc_auc'], cv=cv_strategy, return_train_score=False)
cv_res = cross_validate(pipeline, X_tr, y_tr, scoring=['f1', 'roc_auc'], cv=cv_strategy, return_train_score=False)

for key, value in cv_res.items():
    print(f'avg_{key}: {value.mean().round(2)}')