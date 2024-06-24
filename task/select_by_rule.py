import pandas as pd
import util.config as config

all_funds_info = pd.read_csv(config.ALL_FUND_INFO_FILE, dtype=config.COL_TYPE)

cand_funds_info = all_funds_info[(all_funds_info['近3年'].notnull())
                                 & (all_funds_info['近5年_较同类风险收益比'].notnull())
                                 & (all_funds_info['总募集规模'] > 10000)
                                 & (all_funds_info['基金评级'].isin(['五星基金', '四星基金', '三星基金']))
                                 & (all_funds_info['申购状态'].isin(['开放申购', '限大额']))
                                 & (all_funds_info['购买起点'] <= 1000)
                                 ]

earning_3y_quant = cand_funds_info['近3年'].quantile(0.5)
sharpe_3y_quant = cand_funds_info['近3年_年化夏普比率'].quantile(0.6)
sharpe_5y_quant = cand_funds_info['近5年_年化夏普比率'].quantile(0.6)
drawback_5y_quant = cand_funds_info['近5年_最大回撤'].quantile(0.6)

cand_funds_info = cand_funds_info[(cand_funds_info['近3年'] > earning_3y_quant)
                                  & (cand_funds_info['近3年_年化夏普比率'] > sharpe_3y_quant)
                                  & (cand_funds_info['近5年_年化夏普比率'] > sharpe_5y_quant)
                                  & (cand_funds_info['近5年_最大回撤'] > drawback_5y_quant)
                                  ]

cand_funds_info.to_csv(config.CAND_FUND_INFO_FILE, header=True, index=False)
print()
