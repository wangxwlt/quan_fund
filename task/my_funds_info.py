import pandas as pd
import util.current_info as util
import util.config as config

my_funds = ['012733', '001344', '110020', '005994', '008238', '002656', '007994', '519915', '519002', '007553',
            '003291', '012815', '519097', '012725', '519003', '002084', '004932', '012771', '010364', '519704',
            '004234']
all_funds_info = pd.read_csv(config.ALL_FUND_INFO_FILE, dtype=config.COL_TYPE)
my_funds_info = all_funds_info[all_funds_info[config.INDEX_COL].isin(my_funds)]
my_funds_info.to_csv(config.MY_FUND_INFO_FILE, header=True, index=False)
print()