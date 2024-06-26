import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score


def fit(samples, num_feature_cols, cate_feature_cols, split_func):
    x_train, y_train, x_test, y_test = split_func(samples)
    numerical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value=0.)),
        ('scaler', StandardScaler())
    ])

    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numerical_transformer, num_feature_cols),
            ('cat', categorical_transformer, cate_feature_cols)
        ])

    model = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('classifier', LogisticRegression())
    ])


    model.fit(x_train, y_train)

    y_train_pred = model.predict(x_train)
    accuracy_train = accuracy_score(y_train, y_train_pred)
    auc_train = roc_auc_score(y_train, y_train_pred)

    print("Train Accuracy:", accuracy_train)
    print(f"Train AUC: {auc_train:.2f}")
    print()

    y_test_pred = model.predict(x_test)
    accuracy_test = accuracy_score(y_test, y_test_pred)
    auc_test = roc_auc_score(y_test, y_test_pred)

    print("Test Accuracy:", accuracy_test)
    print(f"Test AUC: {auc_test:.2f}")
    print()

    #analysis
    # 获取处理后的特征
    x_train_processed = model.named_steps['preprocessor'].transform(x_train)
    x_test_processed = model.named_steps['preprocessor'].transform(x_test)

    # 获取处理后的特征维数
    processed_feature_dim = x_train_processed.shape[1]

    # 处理后的特征 DataFrame
    processed_feature_names = (num_feature_cols +
                               list(model.named_steps['preprocessor'].named_transformers_['cat']
                                    .named_steps['onehot'].get_feature_names_out(cate_feature_cols)))
    df_train_processed = pd.DataFrame(x_train_processed, columns=processed_feature_names)
    df_test_processed = pd.DataFrame(x_test_processed, columns=processed_feature_names)

    print(f"Processed feature dimension: {processed_feature_dim}")

    print("\nProcessed feature names:")
    print(processed_feature_names)

    print("\nProcessed training features:")
    print(df_train_processed.describe())

    print("\nProcessed test features:")
    print(df_test_processed.describe())

    # 获取模型的参数
    my_model = model.named_steps['classifier']
    print("\nModel coefficients:")
    print(my_model.coef_)
    print("\nModel intercept:")
    print(my_model.intercept_)
    print()
