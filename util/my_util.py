#!/usr/bin/env python
# -*- coding:utf-8 -*-
import pandas as pd
import requests
from akshare.utils import demjson

from util import config


def fund_individual_detail_info_xq(
        symbol: str = "000001", timeout: float = None
) -> pd.DataFrame:
    """
    雪球基金-交易规则
    https://danjuanfunds.com/djapi/fund/detail/675091
    :param symbol: 基金代码
    :type symbol: str
    :param timeout: choice of None or a positive float number
    :type timeout: float
    :return: 交易规则
    :rtype: pandas.DataFrame
    """
    url = f"https://danjuanfunds.com/djapi/fund/detail/{symbol}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/80.0.3987.149 Safari/537.36"
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    json_data = r.json()["data"]
    combined_df = None
    rate_type_dict = {
        "declare_rate_table": "买入规则",
        "withdraw_rate_table": "卖出规则",
        "other_rate_table": "其他费用",
    }
    for k, v in rate_type_dict.items():
        if k not in json_data["fund_rates"].keys():
            continue
        temp_df = pd.DataFrame.from_dict(json_data["fund_rates"][k], orient="columns")
        temp_df["rate_type"] = v
        temp_df = temp_df[
            [
                "rate_type",
                "name",
                "value",
            ]
        ]
        temp_df.columns = [
            "费用类型",
            "条件或名称",
            "费用",
        ]
        combined_df = pd.concat(objs=[combined_df, temp_df], ignore_index=True)
        combined_df["费用"] = pd.to_numeric(combined_df["费用"], errors="coerce")
    return combined_df


def fund_history_grade_em(
        symbol: str = "000001", timeout: float = None
) -> pd.DataFrame:
    """
    天天基金，基金历史评级
    :param symbol: 基金代码
    :type symbol: str
    :return:
    :rtype: pandas.DataFrame
    """
    url = "https://api.fund.eastmoney.com/F10/JJPJ/"
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "api.fund.eastmoney.com",
        "Pragma": "no-cache",
        "Referer": "http://fundf10.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36",
    }
    params = {
        "fundCode": symbol,
        "pageIndex": 1,
        "pageSize": 100,
        "callback": "jQuery18307678099101196021_1718950461481",
        "_": "1718950461547",
    }
    r = requests.get(url, params=params, headers=headers)
    data_text = r.text
    data_json = demjson.decode(data_text[data_text.find("{"): -1])
    temp_list = []

    for item in data_json["Data"]:
        temp_list.append(item)
    temp_df = pd.DataFrame(temp_list)
    temp_df.columns = [
        config.INDEX_COL,
        "日期",
        "-",
        "招商评级",
        "上海证券评级_3年",
        "上海证券评级_5年",
        "济安金信评级",
        "晨星评级",
    ]
    temp_df = temp_df[
        [
            "日期",
            "招商评级",
            "上海证券评级_3年",
            "上海证券评级_5年",
            "济安金信评级",
            "晨星评级",
        ]
    ]
    return temp_df


def fund_history_net_asset_em(
        symbol: str = "000001"
    ) -> pd.DataFrame:
    """
    天天基金，基金历史净值
    :param symbol: 基金代码
    :type symbol: str
    :return:
    :rtype: pandas.DataFrame
    """
    url = "https://fundf10.eastmoney.com/FundArchivesDatas.aspx"
    headers = {
        "Referer": "http://fundf10.eastmoney.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    }
    params = {
        "code": symbol,
        "type": "gmbd",
        "mode": 0,
        "rt": "0.17769449124583647",
    }
    r = requests.get(url, params=params, headers=headers)
    data_text = r.text
    data_json = demjson.decode(data_text[data_text.find("{"): -1])
    temp_list = []

    for item in data_json["data"]:
        temp_list.append(item)
    temp_df = pd.DataFrame(temp_list)
    temp_df.columns = [
        'id',
        config.INDEX_COL,
        "日期",
        "期末净资产",
        "-",
        "-",
        "净资产变动率",
        "-",
        "期间申购",
        "期间赎回",
        "期末总份额",
        "总份额变动率",
        "-",
        "-",
        "-",
        "-",
        "-",
        "-",
    ]
    temp_df = temp_df[
        [
            config.INDEX_COL,
            "日期",
            "期末净资产",
            "净资产变动率",
            "期间申购",
            "期间赎回",
            "期末总份额",
            "总份额变动率",
        ]
    ]
    return temp_df


