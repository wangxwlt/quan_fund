import sys

import numpy as np
import pandas as pd

import util.current_info as util
import util.common
import util.config as config
import util.my_util as my_util
import model.logistic_regression


def _add_past_return(daily_value, date):
    days = 30
    past_return_df = pd.Series()
    for i in range(1, days+1):
        current_pos = daily_value.index.get_loc(date)
        if current_pos >= i:
            past_return_df[f'前{i}天日增长率'] = daily_value.iloc[current_pos - i]['日增长率']
        else:
            past_return_df[f'前{i}天日增长率'] = np.nan
    return past_return_df


def _make_sample(label_data):
    index = label_data[config.INDEX_COL]
    date = label_data["日期"]
    label_sr = label_data.drop(['基金代码', '日期', '累计净值'])
    index_sr = pd.Series({config.INDEX_COL: index, '样本日期': date})

    static_feature = all_funds_info.loc[index]
    daily_value = all_funds_value.loc[index]
    past_return = _add_past_return(daily_value, date)
    sample_df = pd.concat([index_sr, static_feature, past_return, label_sr])
    # sample_df = sample_df.transpose()
    return sample_df


all_funds_info = pd.read_csv(config.ALL_FUND_INFO_FILE, dtype=config.COL_TYPE)
all_funds_info = all_funds_info.set_index(config.INDEX_COL)
all_funds_info.index.name = config.INDEX_COL
dtype_options = {
    '权益登记日': str,
    '每份分红': str,
    '分红发放日': str,
    '拆分类型': str,
    '拆分折算比例': str
}
dtype_options.update(config.COL_TYPE)
all_funds_value = pd.read_csv(config.FUND_VALUE_FILE, dtype=dtype_options)
all_funds_value = all_funds_value.set_index([config.INDEX_COL, '日期'])
all_funds_value.index.names = [config.INDEX_COL, '日期']
all_funds_label = pd.read_csv(config.FUND_LABEL_FILE, dtype=config.COL_TYPE, nrows=20000)

all_samples = pd.DataFrame()
count = 0
for i, r in all_funds_label.iterrows():
    sample = _make_sample(r)
    sample = sample.to_frame().transpose()
    # sample = sample[['基金代码', '样本日期', '手续费', '总募集规模', '最近总份额', '成立日期',
    #                  '基金经理', '更新日期', '近1年_较同类风险收益比', '近1年_较同类抗风险波动', '近1年_年化波动率',
    #                  '近1年_年化夏普比率', '近1年_最大回撤', '近3年_较同类风险收益比', '近3年_较同类抗风险波动', '近3年_年化波动率',
    #                  '近3年_年化夏普比率', '近3年_最大回撤', '近5年_较同类风险收益比', '近5年_较同类抗风险波动', '近5年_年化波动率',
    #                  '近5年_年化夏普比率', '近5年_最大回撤', '基金公司',
    #                  '托管银行', '评级机构', '基金评级', '投资策略', '投资目标',
    #                  '业绩比较基准', '买入规则', '其他费用', '卖出规则', '申购状态',
    #                  '赎回状态', '下一开放日', '购买起点', '日累计限定金额', '行业']]
    all_samples = pd.concat([all_samples, sample])
    count += 1
    if count % 100 == 0:
        print(count)

util.common.write_csv(all_samples, config.FUND_SAMPLE_FILE)