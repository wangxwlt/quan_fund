import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LinearRegression, ElasticNet
from sklearn.metrics import mean_squared_error, r2_score


def fit(samples, num_feature_cols, cate_feature_cols, split_func):
    # regressor = ElasticNet(alpha=0.1, l1_ratio=0.2)
    regressor = LinearRegression()
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
        ('regressor', regressor)
    ])

    model.fit(x_train, y_train)

    train_y_pred = model.predict(x_train)
    train_mse = mean_squared_error(y_train, train_y_pred)
    train_r2 = r2_score(y_train, train_y_pred)

    print("Train Mean Squared Error:", train_mse)
    print("Train R-squared:", train_r2)
    print()

    test_y_pred = model.predict(x_test)
    test_mse = mean_squared_error(y_test, test_y_pred)
    test_r2 = r2_score(y_test, test_y_pred)

    print("Test Mean Squared Error:", test_mse)
    print("Test R-squared:", test_r2)
    print()
    # model.named_steps.preprocessor.named_transformers_['cat'].named_steps.onehot.categories_

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
    my_model = model.named_steps['regressor']
    print("\nModel coefficients:")
    print(my_model.coef_)
    print("\nModel intercept:")
    print(my_model.intercept_)
    print()
