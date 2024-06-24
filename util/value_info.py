from functools import reduce

import akshare as ak
import pandas as pd

from util import config as config
from util.common import _sleep, _write_csv
from util.current_info import get_fund_names


def get_fund_value(file_path=f'{config.FILE_PREFIX}fund_value.csv', refresh=False, index_list=None):
    print(f"get_fund_value begins!")
    all_fund_values_df = pd.DataFrame()

    if not os.path.exists(file_path) or refresh:
        if index_list is None:
            fund_names = get_fund_names()
            index_list = fund_names[config.INDEX_COL]
        print(f"index_list count = {len(index_list)}")
        total_cnt = 0
        error_cnt = 0
        err_msg = []
        for index in index_list: #.iloc[13000:]:
            total_cnt += 1
            try:
                unit_value = ak.fund_open_fund_info_em(index, indicator="单位净值走势")
                unit_value = unit_value.rename(columns={'净值日期': '日期'})
                unit_value['日期'] = unit_value['日期'].astype(str).str.strip()

                cum_value = ak.fund_open_fund_info_em(index, indicator='累计净值走势')
                cum_value = cum_value.rename(columns={'净值日期': '日期'})
                cum_value['日期'] = cum_value['日期'].astype(str).str.strip()

                cum_return = ak.fund_open_fund_info_em(index, indicator="累计收益率走势", period="成立来")
                cum_return['日期'] = cum_return['日期'].astype(str).str.strip()

                rank_percent = ak.fund_open_fund_info_em(index, indicator='同类排名百分比')
                rank_percent = rank_percent.rename(columns={'报告日期': '日期'})
                rank_percent['日期'] = rank_percent['日期'].astype(str).str.strip()

                divide_info = ak.fund_open_fund_info_em(index, indicator='分红送配详情')
                if divide_info is not None and not divide_info.empty:
                    divide_info = divide_info.rename(columns={'除息日': '日期'})
                    divide_info['日期'] = divide_info['日期'].astype(str).str.strip()
                else:
                    divide_info = pd.DataFrame(columns=['年份', '权益登记日', '日期', '每份分红', '分红发放日'])

                split_info = ak.fund_open_fund_info_em(index, indicator='拆分详情')
                if split_info is not None and not split_info.empty:
                    split_info = split_info.rename(columns={'拆分折算日': '日期'})
                    split_info['日期'] = split_info['日期'].astype(str).str.strip()
                else:
                    split_info = pd.DataFrame(columns=['年份', '日期', '拆分类型', '拆分折算比例'])

                fund_values = [unit_value, cum_value, cum_return, rank_percent, divide_info, split_info]
                fund_values_df = reduce(lambda left, right: pd.merge(left, right, on="日期", how='outer',
                                                                     suffixes=('', '_dup')), fund_values)
                fund_values_df = fund_values_df[fund_values_df['日期'] >= '2019-01-01']
                fund_values_df[config.INDEX_COL] = index
                fund_values_df = fund_values_df[[config.INDEX_COL, '日期', '单位净值', '日增长率', '累计净值', '累计收益率', '同类型排名-每日近3月收益排名百分比',
                                                 '权益登记日', '每份分红', '分红发放日', '拆分类型', '拆分折算比例']]

                all_fund_values_df = pd.concat([all_fund_values_df, fund_values_df])
                print(f"get info of {index} SUCCED")
            except Exception as e:
                error_cnt += 1
                msg = f"get info of {index} failed, {str(e)}"
                print(msg)
                err_msg.append(index + "\t" + msg + "\n")
            finally:
                _sleep(total_cnt)
                if len(all_fund_values_df) > 100000:
                    with open(f'{config.FILE_PREFIX}log.txt', 'a') as f:
                        f.write("------------------------------ Error Message from get_fund_value Begin------------------------------\n")
                        for item in err_msg:
                            f.write(item)
                        err_msg = []
                        f.write("------------------------------ Error Message from get_fund_value End------------------------------\n")
                    # if not all_fund_values_df.empty and config.INDEX_COL not in all_fund_values_df.columns:
                    #     all_fund_values_df = all_fund_values_df.reset_index().rename(columns={'index': config.INDEX_COL})
                    _write_csv(all_fund_values_df, file_path, mode='a')
                    all_fund_values_df = pd.DataFrame()
                    print(f"get from server and write to file")
        print(f"total_cnt={total_cnt}, error_cnt={error_cnt}")
    else:
        all_fund_values_df = pd.read_csv(file_path, dtype=config.COL_TYPE)
        print(f"get from local")
    print(f"get_fund_value done! count={all_fund_values_df.shape[0]}")
    print()
    return all_fund_values_df
