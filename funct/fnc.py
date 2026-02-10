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