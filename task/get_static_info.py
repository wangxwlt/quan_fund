import util.current_info as util
import pandas as pd
from functools import reduce
import util.config as config

fund_names = util.get_fund_names()
fund_ranks = util.get_fund_ranks()
fund_scales = util.get_fund_scales()
fund_analysis = util.get_fund_analysis()
fund_infos = util.get_fund_infos()
fund_rules = util.get_fund_rules()
fund_status = util.get_fund_status()
fund_industry = util.get_fund_industry()

all_fund_info_list = [fund_names, fund_ranks, fund_scales, fund_analysis, fund_infos, fund_rules, fund_status, fund_industry]
all_fund_info_df = reduce(lambda left, right: pd.merge(left, right, on=config.INDEX_COL, how='outer', suffixes=('', '_dup')),
                          all_fund_info_list)
all_fund_info_df = all_fund_info_df.loc[:, ~all_fund_info_df.columns.duplicated()]
all_fund_info_df = all_fund_info_df[['基金代码', '基金简称', '基金类型', '日期', '单位净值',
                                     '累计净值', '日增长率', '近1周', '近1月', '近3月', '近6月', '近1年', '近2年', '近3年', '今年来',
                                     '成立来', '手续费', '总募集规模', '最近总份额', '成立日期',
                                     '基金经理', '更新日期', '近1年_较同类风险收益比', '近1年_较同类抗风险波动', '近1年_年化波动率',
                                     '近1年_年化夏普比率', '近1年_最大回撤', '近3年_较同类风险收益比', '近3年_较同类抗风险波动', '近3年_年化波动率',
                                     '近3年_年化夏普比率', '近3年_最大回撤', '近5年_较同类风险收益比', '近5年_较同类抗风险波动', '近5年_年化波动率',
                                     '近5年_年化夏普比率', '近5年_最大回撤', '基金公司',
                                     '托管银行', '评级机构', '基金评级', '投资策略', '投资目标',
                                     '业绩比较基准', '买入规则', '其他费用', '卖出规则', '申购状态',
                                     '赎回状态', '下一开放日', '购买起点', '日累计限定金额', '行业']]
all_fund_info_df = all_fund_info_df.drop_duplicates()
all_fund_info_df.to_csv(config.ALL_FUND_INFO_FILE, mode='w', header=True, index=False)
print()