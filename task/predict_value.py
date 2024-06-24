import pandas as pd
import model.logistic_regression
import model.linear_regression
import util.config as config
pd.set_option('display.max_columns', None)

all_samples = pd.read_csv(config.FUND_SAMPLE_FILE, dtype=config.COL_TYPE)
all_samples['手续费'] = all_samples['手续费'].str.rstrip('%').astype('float')
features_cols = ["基金类型", "手续费", "基金经理", "基金公司", "前1天日增长率", "前2天日增长率", "前3天日增长率", "前4天日增长率",
                 "前5天日增长率", "前6天日增长率", "前7天日增长率", "前8天日增长率", "前9天日增长率", "前10天日增长率",
                 "前11天日增长率", "前12天日增长率", "前13天日增长率", "前14天日增长率", "前15天日增长率", "前16天日增长率",
                 "前17天日增长率", "前18天日增长率", "前19天日增长率", "前20天日增长率", "前21天日增长率", "前22天日增长率",
                 "前23天日增长率", "前24天日增长率", "前25天日增长率", "前26天日增长率", "前27天日增长率", "前28天日增长率",
                 "前29天日增长率", "前30天日增长率"
                 ]
cate_feature_cols = ["基金类型", "基金经理", "基金公司"]
num_feature_cols = list(set(features_cols) - set(cate_feature_cols))
# all_samples_feature = all_samples[features_cols]
# all_samples_feature[cate_feature_cols] = all_samples_feature[cate_feature_cols].fillna("null", inplace=True)
# all_samples_feature[continuous_feature_cols] = all_samples_feature[continuous_feature_cols].fillna(0., inplace=True)
# all_samples_label = all_samples['label']


def split_func(samples):
    split_date = '2019-08-01'
    label_str = '7天后累计净值正负'
    # label_str = '7天后累计净值变化率'
    samples = samples.dropna(subset=[label_str])
    samples_train_df = samples[samples['样本日期'] < split_date]
    samples_test_df = samples[samples['样本日期'] >= split_date]
    x_train = samples_train_df[num_feature_cols + cate_feature_cols]
    y_train = samples_train_df[label_str]
    x_test = samples_test_df[num_feature_cols + cate_feature_cols]
    y_test = samples_test_df[label_str]
    return x_train, y_train, x_test, y_test


model.logistic_regression.fit(all_samples, num_feature_cols, cate_feature_cols, split_func)
# model.linear_regression.fit(all_samples, num_feature_cols, cate_feature_cols, split_func)
print()
