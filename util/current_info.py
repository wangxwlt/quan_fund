import json
import os.path

import akshare as ak
import pandas as pd
from collections import OrderedDict
import util.config as config
from util.common import _sleep, _check_merge_by_index

pd.set_option('display.max_columns', None)


def get_fund_names(file_path=f'{config.FILE_PREFIX}fund_name.csv', refresh=False):
    print(f"get_fund_names begins!")
    server_fund_names = pd.DataFrame()
    local_fund_names = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_names = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        server_fund_names = ak.fund_name_em()

    merged_fund_names = _check_merge_by_index(server_fund_names, local_fund_names, file_path)
    print(f"get_fund_names done! count={merged_fund_names.shape[0]}")
    print()
    return merged_fund_names


def get_fund_ranks(file_path=f'{config.FILE_PREFIX}fund_rank.csv', refresh=False):
    print(f"get_fund_ranks begins!")
    server_fund_ranks = pd.DataFrame()
    local_fund_ranks = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_ranks = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        # fund_ratings = ak.fund_rating_all()
        server_fund_ranks = ak.fund_open_fund_rank_em()

    merged_fund_ranks = _check_merge_by_index(server_fund_ranks, local_fund_ranks, file_path)
    print(f"get_fund_ranks done! count={merged_fund_ranks.shape[0]}")
    print()
    return merged_fund_ranks


def get_fund_scales(file_path=f'{config.FILE_PREFIX}fund_scale.csv', refresh=False):
    print(f"get_fund_scales begins!")
    server_fund_scales = pd.DataFrame()
    local_fund_scales = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_scales = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        fund_types = ["股票型基金", "混合型基金", "债券型基金", "货币型基金", "QDII基金"]
        for t in fund_types:
            tmp_scale = ak.fund_scale_open_sina(t)
            server_fund_scales = pd.concat([server_fund_scales, tmp_scale])

    merged_fund_scales = _check_merge_by_index(server_fund_scales, local_fund_scales, file_path)
    print(f"get_fund_scales done! count={merged_fund_scales.shape[0]}")
    print()
    return merged_fund_scales


def get_fund_infos(file_path=f'{config.FILE_PREFIX}fund_info.csv', refresh=False, index_list=None):
    print(f"get_fund_infos begins!")
    server_fund_infos = pd.DataFrame()
    local_fund_infos = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_infos = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        if index_list is None:
            fund_names = get_fund_names()
            index_list = fund_names[config.INDEX_COL]
        total_cnt = 0
        error_cnt = 0
        err_msg = []
        for index in index_list: #.iloc[13000:]:
            total_cnt += 1
            try:
                tmp_basic_info = ak.fund_individual_basic_info_xq(index, config.TIMEOUT).set_index('item').T
                server_fund_infos = pd.concat([server_fund_infos, tmp_basic_info])
            except Exception as e:
                error_cnt += 1
                msg = f"get info of {index} failed, {str(e)}"
                # print(msg)
                err_msg.append(index + "\t" + msg + "\n")
            finally:
                _sleep(total_cnt)
        with open(f'{config.FILE_PREFIX}log.txt', 'a') as f:
            f.write("------------------------------ Error Message from get_fund_infos Begin------------------------------\n")
            for item in err_msg:
                f.write(item)
            f.write("------------------------------ Error Message from get_fund_infos End------------------------------\n")
        if not server_fund_infos.empty and config.INDEX_COL not in server_fund_infos.columns:
            server_fund_infos = server_fund_infos.reset_index().rename(columns={'index': config.INDEX_COL})
        print(f"total_cnt={total_cnt}, error_cnt={error_cnt}")

    merged_fund_infos = _check_merge_by_index(server_fund_infos, local_fund_infos, file_path)
    print(f"get_fund_infos done! count={merged_fund_infos.shape[0]}")
    print()
    return merged_fund_infos


def get_fund_analysis(file_path=f'{config.FILE_PREFIX}fund_analysis.csv', refresh=False, index_list=None):
    print(f"get_fund_analysis begins!")
    server_fund_analysis = pd.DataFrame()
    local_fund_analysis = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_analysis = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        if index_list is None:
            fund_names = get_fund_names()
            index_list = fund_names[config.INDEX_COL]
        total_cnt = 0
        error_cnt = 0
        err_msg = []
        for index in index_list: #.iloc[13000:]:
            total_cnt += 1
            try:
                tmp_analysis = ak.fund_individual_analysis_xq(index, config.TIMEOUT)
                new_analysis = {index: {}}
                for _, row in tmp_analysis.iterrows():
                    period = row['周期']
                    for col_name in tmp_analysis.columns[1:]:
                        new_col_name = f"{period}_{col_name}"
                        new_analysis[index][new_col_name] = row[col_name]
                new_analysis_df = pd.DataFrame.from_dict(new_analysis, orient='index')
                server_fund_analysis = pd.concat([server_fund_analysis, new_analysis_df])
                # print(f"get info of {index} SUCCED")
            except Exception as e:
                error_cnt += 1
                msg = f"get info of {index} failed, {str(e)}"
                # print(msg)
                err_msg.append(index + "\t" + msg + "\n")
            finally:
                _sleep(total_cnt)
        with open(f'{config.FILE_PREFIX}log.txt', 'a') as f:
            f.write("------------------------------ Error Message from get_fund_analysis Begin------------------------------\n")
            for item in err_msg:
                f.write(item)
            f.write("------------------------------ Error Message from get_fund_analysis End------------------------------\n")
        if not server_fund_analysis.empty and config.INDEX_COL not in server_fund_analysis.columns:
            server_fund_analysis = server_fund_analysis.reset_index().rename(columns={'index': config.INDEX_COL})
        print(f"total_cnt={total_cnt}, error_cnt={error_cnt}")

    merged_fund_analysis = _check_merge_by_index(server_fund_analysis, local_fund_analysis, file_path)
    print(f"get_fund_analysis done! count={merged_fund_analysis.shape[0]}")
    print()
    return merged_fund_analysis


def get_fund_rules(file_path=f'{config.FILE_PREFIX}fund_rule.csv', refresh=False, index_list=None):
    print(f"get_fund_rules begins!")
    server_fund_rules = pd.DataFrame()
    local_fund_rules = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_rules = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        if index_list is None:
            fund_names = get_fund_names()
            index_list = fund_names[config.INDEX_COL]
        total_cnt = 0
        error_cnt = 0
        err_msg = []
        for index in index_list: #.iloc[13000:]:
            total_cnt += 1
            try:
                def concat_conditions_fees(group):
                    return group.apply(lambda x: f"{x['条件或名称']}：{x['费用']}", axis=1).tolist()
                tmp_trans_rule = ak.fund_individual_detail_info_xq(index, config.TIMEOUT)
                tmp_trans_rule = tmp_trans_rule.groupby('费用类型').apply(concat_conditions_fees).reset_index()
                tmp_trans_rule.columns = ['费用类型', '条件及费用']
                new_trans_rule = {index: {}}
                for _, row in tmp_trans_rule.iterrows():
                    new_col_name = row['费用类型']
                    new_trans_rule[index][new_col_name] = row['条件及费用']
                new_trans_rule_df = pd.DataFrame.from_dict(new_trans_rule, orient='index')
                server_fund_rules = pd.concat([server_fund_rules, new_trans_rule_df])
            except Exception as e:
                error_cnt += 1
                msg = f"get info of {index} failed, {str(e)}"
                # print(msg)
                err_msg.append(index + "\t" + msg + "\n")
            finally:
                _sleep(total_cnt)
        with open(f'{config.FILE_PREFIX}log.txt', 'a') as f:
            f.write("------------------------------ Error Message from get_fund_rules Begin------------------------------\n")
            for item in err_msg:
                f.write(item)
            f.write("------------------------------ Error Message from get_fund_rules End------------------------------\n")
        if not server_fund_rules.empty and config.INDEX_COL not in server_fund_rules.columns:
            server_fund_rules = server_fund_rules.reset_index().rename(columns={'index': config.INDEX_COL})
        print(f"total_cnt={total_cnt}, error_cnt={error_cnt}")

    merged_fund_rules = _check_merge_by_index(server_fund_rules, local_fund_rules, file_path)
    print(f"get_fund_rules done! count={merged_fund_rules.shape[0]}")
    print()
    return merged_fund_rules


def get_fund_status(file_path=f'{config.FILE_PREFIX}fund_status.csv', refresh=False):
    print(f"get_fund_status begins!")
    server_fund_status = pd.DataFrame()
    local_fund_status = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_status = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        server_fund_status = ak.fund_purchase_em()

    merged_fund_status = _check_merge_by_index(server_fund_status, local_fund_status, file_path)
    print(f"get_fund_status done! count={merged_fund_status.shape[0]}")
    print()
    return merged_fund_status


def get_fund_industry(file_path=f'{config.FILE_PREFIX}fund_industry.csv', refresh=False, index_list=None):
    print(f"get_fund_industry begins!")
    server_fund_industry = pd.DataFrame()
    local_fund_industry = pd.DataFrame()

    if not os.path.exists(file_path):
        refresh = True
    else:
        local_fund_industry = pd.read_csv(file_path, dtype=config.COL_TYPE)

    if refresh:
        if index_list is None:
            fund_names = get_fund_names()
            index_list = fund_names[config.INDEX_COL]
        total_cnt = 0
        error_cnt = 0
        err_msg = []
        for index in index_list: #.iloc[:100]:
            total_cnt += 1
            try:
                year = '2024'
                tmp_industry = ak.fund_portfolio_industry_allocation_em(index, year)
                tmp_industry = tmp_industry[tmp_industry['截止时间'] == tmp_industry['截止时间'].max()]
                new_industry = {index: {}}
                industry_dict = OrderedDict()
                industry_cnt = 10
                for _, row in tmp_industry.iterrows():
                    industry_dict[row['行业类别']] = row['占净值比例']
                    industry_cnt -= 1
                    if industry_cnt == 0:
                        break
                new_industry[index]['行业'] = json.dumps(industry_dict, ensure_ascii=False)
                new_industry_df = pd.DataFrame.from_dict(new_industry, orient='index')
                server_fund_industry = pd.concat([server_fund_industry, new_industry_df])
            except Exception as e:
                error_cnt += 1
                msg = f"get info of {index} failed, {str(e)}"
                # print(msg)
                err_msg.append(index + "\t" + msg + "\n")
            finally:
                _sleep(total_cnt)
        with open(f'{config.FILE_PREFIX}log.txt', 'a') as f:
            f.write("------------------------------ Error Message from get_fund_rules Begin------------------------------\n")
            for item in err_msg:
                f.write(item)
            f.write("------------------------------ Error Message from get_fund_rules End------------------------------\n")
        if not server_fund_industry.empty and config.INDEX_COL not in server_fund_industry.columns:
            server_fund_industry = server_fund_industry.reset_index().rename(columns={'index': config.INDEX_COL})
        print(f"total_cnt={total_cnt}, error_cnt={error_cnt}")

    merged_fund_industry = _check_merge_by_index(server_fund_industry, local_fund_industry, file_path)
    print(f"get_fund_rules done! count={merged_fund_industry.shape[0]}")
    print()
    return merged_fund_industry


