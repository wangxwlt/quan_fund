import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.impute import SimpleImputer

# 生成样本数据
np.random.seed(0)
num_samples = 100

# 连续特征
X_continuous = np.random.rand(num_samples, 2)  # 两个连续特征
# 分类特征
X_categorical = np.random.choice(['A', 'B', 'C'], size=(num_samples, 1))  # 一个分类特征

# 在数据中引入一些缺失值
X_continuous[np.random.choice(num_samples, 10), 0] = np.nan
X_categorical[np.random.choice(num_samples, 10), 0] = np.nan

# 特征组合
X = np.hstack([X_continuous, X_categorical])

# 目标
y_classification = np.random.randint(0, 2, size=(num_samples, 1))  # 0/1 分类目标
y_regression = np.random.rand(num_samples, 1) * 100  # 实数值目标

# 创建 DataFrame
df = pd.DataFrame(X, columns=['cont1', 'cont2', 'cat1'])
df['class_target'] = y_classification
df['reg_target'] = y_regression
