import sys
import pandas as pd
import util.config as config


def _custom_clip(x, thresh_dict=None):
    if thresh_dict is None:
        thresh_dict = {-0.1: -1, 0.1: 0, sys.float_info.max: 1}
    for thresh, value in thresh_dict.items():
        if x < thresh:
            return value


fund_value = pd.read_csv(config.FUND_VALUE_FILE, dtype=config.COL_TYPE) #, nrows=10000)
fund_value = fund_value[['基金代码', '日期', '累计净值']]
# fund_value['日期'] = pd.to_datetime(fund_value['日期'])
fund_value = fund_value.sort_values(by=['基金代码', '日期'])
fund_value['1天后累计净值变化率'] = (fund_value.groupby('基金代码')['累计净值'].shift(-1) - fund_value['累计净值']) / fund_value['累计净值']
fund_value['1天后累计净值正负'] = (fund_value['1天后累计净值变化率'] > 0.0).astype(int)
thresh_1d = 0.0002
fund_value['1天后累计净值类别'] = fund_value['1天后累计净值变化率'].apply(
    lambda x: _custom_clip(x, thresh_dict={thresh_1d * -1: -1, thresh_1d: 0, sys.float_info.max: 1}))

fund_value['7天后累计净值变化率'] = (fund_value.groupby('基金代码')['累计净值'].shift(-7) - fund_value['累计净值']) / fund_value['累计净值']
fund_value['7天后累计净值正负'] = (fund_value['7天后累计净值变化率'] > 0.0).astype(int)
thresh_7d = 0.00125
fund_value['7天后累计净值类别'] = fund_value['7天后累计净值变化率'].apply(
    lambda x: _custom_clip(x, thresh_dict={thresh_7d * -1: -1, thresh_7d: 0, sys.float_info.max: 1}))

fund_value.to_csv(config.FUND_LABEL_FILE, mode="w", header=True, index=False)
# print(fund_value)