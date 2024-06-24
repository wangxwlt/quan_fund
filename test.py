import time

import akshare as ak
import pandas as pd
import util.my_util as my_ak

pd.set_option('display.max_columns', None)

timeout = 100
total_cnt = 0
error_cnt = 0
csv_file = 'data/test.csv'
# '000002', '000008',
fund_indexes = ['000044', '000459', '002426', '010670', '011647', '012306', '017835']
fund_basic_infos = pd.DataFrame()
fund_analysis = pd.DataFrame()
fund_trans_rules = pd.DataFrame()

all_cols = ["基金代码", "基金名称", "基金全称", "成立时间", "最新规模", "基金公司", "基金经理", "托管银行", "基金类型", "评级机构", "基金评级", "投资策略", "投资目标", "业绩比较基准", "近1年_较同类风险收益比", "近1年_较同类抗风险波动", "近1年_年化波动率", "近1年_年化夏普比率", "近1年_最大回撤", "近3年_较同类风险收益比", "近3年_较同类抗风险波动", "近3年_年化波动率", "近3年_年化夏普比率", "近3年_最大回撤", "近5年_较同类风险收益比", "近5年_较同类抗风险波动", "近5年_年化波动率", "近5年_年化夏普比率", "近5年_最大回撤", "买入规则", "其他费用", "卖出规则"]
for index in fund_indexes:
    total_cnt += 1

    tmp_basic_info = ak.fund_individual_basic_info_xq(index, timeout).set_index('item').T
    fund_basic_infos = pd.concat([fund_basic_infos, tmp_basic_info])
    try:
        pass
        # print(f"fund {index} done")
    except Exception as e:
        error_cnt += 1
        msg = f"basic info: fund {index} failed, {str(e)}"
        print(msg)

    tmp_analysis = ak.fund_individual_analysis_xq(index, timeout)
    new_analysis = {index: {}}
    for _, row in tmp_analysis.iterrows():
        period = row['周期']
        for col_name in tmp_analysis.columns[1:]:
            new_col_name = f"{period}_{col_name}"
            new_analysis[index][new_col_name] = row[col_name]
    new_analysis_df = pd.DataFrame.from_dict(new_analysis, orient='index')
    fund_analysis = pd.concat([fund_analysis, new_analysis_df])
    try:
        pass
    except Exception as e:
        error_cnt += 1
        msg = f"analysis: fund {index} failed, {str(e)}"
        print(msg)

    def concat_conditions_fees(group):
        return group.apply(lambda x: f"{x['条件或名称']}：{x['费用']}", axis=1).tolist()
    tmp_trans_rule = my_ak.fund_individual_detail_info_xq(index, timeout)
    tmp_trans_rule = tmp_trans_rule.groupby('费用类型').apply(concat_conditions_fees).reset_index()
    tmp_trans_rule.columns = ['费用类型', '条件及费用']
    new_trans_rule = {index: {}}
    for _, row in tmp_trans_rule.iterrows():
        new_col_name = row['费用类型']
        new_trans_rule[index][new_col_name] = row['条件及费用']
    new_trans_rule_df = pd.DataFrame.from_dict(new_trans_rule, orient='index')
    fund_trans_rules = pd.concat([fund_trans_rules, new_trans_rule_df])
    try:
        pass
    except Exception as e:
        error_cnt += 1
        msg = f"trans rules: fund {index} failed, {str(e)}"
        print(msg)
    print()
