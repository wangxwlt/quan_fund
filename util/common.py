import os
import time

import pandas as pd

from util import config as config


def _write_csv(df, csv_file, mode='w'):
    if not df.empty:
        if config.INDEX_COL not in df.columns:
            df = df.reset_index().rename(columns={'index': config.INDEX_COL})
        if os.path.isfile(csv_file):
            header = False
        else:
            header = True
        df.to_csv(csv_file, mode=mode, header=header, index=False)
        # try:
        #     with open(csv_file, 'r') as f:
        #         df.to_csv(csv_file, mode='a', header=False, index=False)
        # except FileNotFoundError:
        #     df.to_csv(csv_file, mode='w', header=True, index=False)


def _sleep(total_cnt):
    if total_cnt % 1000 == 0:
        print(f"total_count={total_cnt}")
        time.sleep(1)
    # elif total_cnt % 1000 == 0:
    #     time.sleep(60)
    # elif total_cnt % 10000 == 0:
    #     time.sleep(300)


def _combine_first(first_df, second_df):
    first_df.set_index(config.INDEX_COL, inplace=True)
    second_df.set_index(config.INDEX_COL, inplace=True)
    merged_fund_names = first_df.combine_first(second_df).reset_index()
    return merged_fund_names


def _check_merge_by_index(first_df, second_df, file_path):
    merged_df = pd.DataFrame()
    if not first_df.empty and not second_df.empty:
        merged_df = _combine_first(first_df, second_df)
        _write_csv(merged_df, file_path)
        print(f"[_check_merge] combine server and local, write to file")
    elif not first_df.empty:
        merged_df = first_df
        _write_csv(merged_df, file_path)
        print(f"[_check_merge] get from server and write to file")
    elif not second_df.empty:
        merged_df = second_df
        print(f"[_check_merge] get from local")
    else:
        print(f"[_check_merge] no items found")
    return merged_df


def write_csv(df, csv_file, mode='w'):
    _write_csv(df, csv_file, mode)
