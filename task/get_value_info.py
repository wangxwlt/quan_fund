import util.value_info as value_info
import pandas as pd

import util.common as common
import util.config as config

file_path = "/Users/xiaoweiwang/Codes/quan_fund/data/all_fund_info.csv"
all_funds_info = pd.read_csv(config.ALL_FUND_INFO_FILE, dtype=config.COL_TYPE)
cand_funds_info = all_funds_info[(all_funds_info['近3年'].notnull())
                                 & (all_funds_info['总募集规模'] > 10000)
                                 & (all_funds_info['基金评级'].isin(['五星基金', '四星基金', '三星基金']))
                                 & (all_funds_info['购买起点'] <= 1000)
                                 ]
index_list = cand_funds_info[config.INDEX_COL]
# index_list = ['002656', '012733', '001344', '110020', '005994', '008238', '002656', '007994', '519915', '519002', '007553',
#               '003291', '012815', '519097', '012725', '519003', '002084', '004932', '012771', '010364', '519704',
#               '004234', '003949']
fund_values = value_info.get_fund_value(index_list=index_list)

fund_values = fund_values.drop_duplicates()
common.write_csv(fund_values, config.FUND_VALUE_FILE, mode='w')
print()
