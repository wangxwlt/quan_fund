import datetime
from dateutil.relativedelta import relativedelta
from enum import Enum

import akshare as ak
import pandas as pd
from pandas import DataFrame
from loguru import logger


class ReportPeriod(Enum):
    YEARLY = 1
    QUARTERLY = 2
    BY_REPORT = 3


class AkshareDataset:
    def __init__(self):
        pass

    @staticmethod
    def sh_stock_names() -> pd.DataFrame:
        stock_names = ak.stock_info_sh_name_code()
        return stock_names

    @staticmethod
    def sz_stock_names() -> pd.DataFrame:
        stock_names = ak.stock_info_sz_name_code()
        return stock_names

    @staticmethod
    def a_stock_names() -> pd.DataFrame:
        sh_stock_names = AkshareDataset.sh_stock_names()
        sh_stock_names = pd.DataFrame({
            'stock_code': sh_stock_names['证券代码'],
            'stock_name': sh_stock_names['证券简称'],
            'begin_date': sh_stock_names['上市日期'],
        })
        sz_stock_names = AkshareDataset.sz_stock_names()
        sz_stock_names = pd.DataFrame({
            'stock_code': sz_stock_names['A股代码'],
            'stock_name': sz_stock_names['A股简称'],
            'begin_date': sz_stock_names['A股上市日期']
        })
        return pd.concat([sh_stock_names, sz_stock_names])

    @staticmethod
    def stock_spot():
        dataset = ak.stock_zh_a_spot_em()
        column_translation = {
            '序号': 'index',
            '代码': 'stock_id',
            '名称': 'stock_name',
            '最新价': 'price',
            '涨跌幅': 'ratio',
            '涨跌额': 'ratio_value',
            '成交量': 'volume',
            '成交额': 'volume_value',
            '振幅': 'amplitude',
            '最高': 'high',
            '最低': 'low',
            '今开': 'today_open',
            '昨收': 'yesterday_close',
            '量比': 'volume_ratio',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe',
            '市净率': 'pb',
            '总市值': 'market_value',
            '流通市值': 'flow_market_value',
            '涨速': 'ratio_acc',
            '5分钟涨跌': '5m_ret',
            '60日涨跌幅': '60d_ret',
            '年初至今涨跌幅': 'year_ret',
        }
        for old_col, new_col in column_translation.items():
            if old_col in dataset.columns:
                dataset.rename(columns={old_col: new_col}, inplace=True)
        return dataset

    @staticmethod
    def stock_timed(stock_id: str, start_date: str, end_date: str, period: str) -> DataFrame:
        stock = ak.stock_zh_a_hist(symbol=stock_id, period=period, start_date=start_date, end_date=end_date)
        column_translation = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'turnover',
            '振幅': 'amplitude',
            '涨跌幅': 'change_percentage',
            '涨跌额': 'change',
            '换手率': 'turnover_rate'
        }
        for old_col, new_col in column_translation.items():
            if old_col in stock.columns:
                stock.rename(columns={old_col: new_col}, inplace=True)
        return stock

    @staticmethod
    def stock_date(stock_id: str, start_date: str = '', end_date: str = '') -> DataFrame:
        if start_date == '' and end_date == '':
            stock = ak.stock_zh_a_daily(symbol=stock_id)
        else:
            stock = ak.stock_zh_a_daily(symbol=stock_id, start_date=start_date, end_date=end_date)
        return stock

    @staticmethod
    def stock_minute(stock_id: str, period: str) -> DataFrame:
        stock = ak.stock_zh_a_minute(symbol=stock_id, period=period)
        return stock

    @staticmethod
    def stock_basic_finance_index(stock_id: str) -> pd.DataFrame:
        data = ak.stock_a_indicator_lg(stock_id)
        return data

    @staticmethod
    def stock_profit(stock_id: str, period: ReportPeriod = ReportPeriod.QUARTERLY) -> DataFrame:
        profit_table = None
        print(f"===> begin querying profit table of {stock_id} <===")
        if period == ReportPeriod.QUARTERLY:
            profit_table = ak.stock_profit_sheet_by_quarterly_em(symbol=stock_id)
        elif period == ReportPeriod.YEARLY:
            profit_table = ak.stock_profit_sheet_by_yearly_em(symbol=stock_id)
        elif period == ReportPeriod.BY_REPORT:
            profit_table = ak.stock_profit_sheet_by_report_em(symbol=stock_id)
        print(f"===> end querying profit table of {stock_id} <===")
        return profit_table

    @staticmethod
    def stock_balance(stock_id: str, period: ReportPeriod = ReportPeriod.QUARTERLY) -> DataFrame:
        balance_table = None
        print(f"===> begin querying balance table of {stock_id} <===")
        if period == ReportPeriod.QUARTERLY:
            print("Err: we don't support querying balance table by quarter")
        elif period == ReportPeriod.YEARLY:
            balance_table = ak.stock_balance_sheet_by_yearly_em(symbol=stock_id)
        elif period == ReportPeriod.BY_REPORT:
            balance_table = ak.stock_balance_sheet_by_report_em(symbol=stock_id)
        print(f"===> end querying balance table of {stock_id} <===")
        return balance_table

    @staticmethod
    def stock_cash_flow(stock_id: str, period: ReportPeriod = ReportPeriod.QUARTERLY) -> DataFrame:
        cash_flow_table = None
        print(f"===> begin querying cash flow table of {stock_id} <===")
        if period == ReportPeriod.QUARTERLY:
            cash_flow_table = ak.stock_cash_flow_sheet_by_quarterly_em(symbol=stock_id)
        elif period == ReportPeriod.YEARLY:
            cash_flow_table = ak.stock_cash_flow_sheet_by_yearly_em(symbol=stock_id)
        elif period == ReportPeriod.BY_REPORT:
            cash_flow_table = ak.stock_cash_flow_sheet_by_report_em(symbol=stock_id)
        print(f"===> end querying cash flow table of {stock_id} <===")
        return cash_flow_table

    @staticmethod
    def stock_rank(date: str) -> DataFrame:
        return ak.stock_rank_forecast_cninfo(date)

    @staticmethod
    def stock_price_finance(stock_id: str, start_date: datetime) -> pd.DataFrame:
        # transform prefix stock id
        prefix_stock_id = ''
        if stock_id.startswith('6'):
            prefix_stock_id = 'sh' + stock_id
        elif stock_id.startswith('0') or stock_id.startswith('3'):
            prefix_stock_id = 'sz' + stock_id
        price_dataset = ak.stock_date(prefix_stock_id)
        finance_dataset = ak.stock_basic_finance_index(stock_id)
        # transform timestamp
        start_timestamp = start_date.strftime('%Y-%m-%d')
        fin_start_date = finance_dataset['trade_date'].tolist()[0]
        price_start_date = price_dataset['date'].tolist()[0].date()
        result_start_date = start_date.date()
        if result_start_date < fin_start_date:
            result_start_date = fin_start_date
        if result_start_date < price_start_date:
            result_start_date = price_start_date
        # pre-handle
        finance_dataset.rename(columns={'trade_date': 'date'}, inplace=True)
        finance_dataset = finance_dataset[finance_dataset['date'] >= result_start_date]
        finance_dataset = finance_dataset.fillna(value=0)
        price_dataset = price_dataset[
            price_dataset['date'] >= datetime(result_start_date.year, result_start_date.month, result_start_date.day)]
        price_dataset = price_dataset.fillna(value=0)
        # transform index
        finance_dataset['date'] = pd.to_datetime(finance_dataset['date'], format="%Y-%m-%d")
        price_dataset['date'] = pd.to_datetime(price_dataset['date'], format="%Y-%m-%d")
        result_dataset = pd.merge(price_dataset, finance_dataset, how='inner', on='date')

        result_dataset['ret'] = result_dataset['close'].pct_change(1)
        result_dataset.insert(loc=2, column='stock_id', value=stock_id)
        result_dataset = result_dataset.fillna(value=0)
        return result_dataset


    @staticmethod
    def bond_spot(bond_id: str) -> DataFrame:
        codes = bond_id.split('.')
        if codes[1] == 'SH':
            bond_id = 'sh' + codes[0]
        elif codes[1] == 'SZ':
            bond_id = 'sz' + codes[0]
        dataset = ak.bond_zh_hs_cov_spot()
        return dataset[dataset['symbol'] == bond_id]

    @staticmethod
    def bond_date(bond_id: str, start_date: str, end_date: str) -> DataFrame:
        dataset = ak.bond_zh_hs_daily(bond_id)
        return dataset

    @staticmethod
    def bond_info_all_jsl(cookie: str) -> DataFrame:
        dataset = ak.bond_cb_jsl()
        return dataset

    @staticmethod
    def bond_info_all() -> DataFrame:
        ak.bond_cb_jsl()
        dataset = ak.bond_zh_cov()
        dataset = pd.DataFrame({
            'bond_id': dataset['债券代码'],
            'buy_date': dataset['申购日期'],
            'stock_id': dataset['正股代码'],
            'price': dataset['债现价'],
            'transform_ratio': dataset['转股溢价率']
        })

        # dataset = pd.read_csv(r'D:\program\python\my-quant-dev\libs\open-quant-data\assets\output\bond_info.csv')

        # drop nan
        dataset = dataset.dropna(axis=0, how='any')

        # drop bond.bk
        dataset = dataset[~dataset['bond_id'].str.startswith('40')]

        # drop price nan
        dataset = dataset[dataset['price'] > 0]

        # add bond id suffix
        def add_bond_suffix(bond: int):
            if str(bond).startswith('12'):
                return f"{bond}.SZ"
            elif str(bond).startswith('11'):
                return f"{bond}.SH"
            else:
                return bond

        dataset['bond_id'] = dataset['bond_id'].apply(add_bond_suffix)

        # add stock id suffix
        def add_stock_suffix(stock: int):
            if str(stock).startswith('600') or str(stock).startswith('601') or str(stock).startswith('60') \
                    or str(stock).startswith('688'):
                return f"{stock}.SH"
            elif str(stock).startswith('000') or str(stock).startswith('300') or str(stock).startswith('001') \
                    or str(stock).startswith('00') or str(stock).startswith('30'):
                return f"{stock}.SZ"
            else:
                return stock

        dataset['stock_id'] = dataset['stock_id'].apply(add_stock_suffix)
        return dataset

    @staticmethod
    def fund_daily(fund_id: str) -> pd.DataFrame:
        dataset = ak.fund_etf_fund_daily_em()
        return dataset

    @staticmethod
    def fund_timed(fund_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        dataset = ak.fund_etf_fund_info_em(fund=fund_id, start_date=start_date, end_date=end_date)
        column_translation = {
            '净值日期': 'date',
            '单位净值': 'unit_equity',
            '累计净值': 'cumulative_equity',
            '日增长率': 'increase_rate',
            '申购状态': 'subscription_status',
            '赎回状态': 'redemption_status',
        }
        for old_col, new_col in column_translation.items():
            if old_col in dataset.columns:
                dataset.rename(columns={old_col: new_col}, inplace=True)
        return dataset

    @staticmethod
    def between_date(data: DataFrame, start_date: str, end_date: str, date_col_name='date') -> DataFrame:
        filtered_data = data[(data[date_col_name] >= start_date) & (data[date_col_name] <= end_date)]
        return filtered_data

    @staticmethod
    def prev_date(data: DataFrame, start_date: str, date_col_name='date') -> DataFrame:
        filtered_data = data[(data[date_col_name] >= start_date)]
        return filtered_data

    @staticmethod
    def prev_n_month(data: DataFrame, n: int, date_col_name='date') -> DataFrame:
        curr_date = datetime.datetime.now()
        target_date = curr_date - relativedelta(months=n)
        target_date_prev = curr_date - relativedelta(months=n - 1)
        target_date_str = target_date.strftime("%Y-%m-01")
        target_date_prev_str = target_date_prev.strftime("%Y-%m-01")
        return AkshareDataset.between_date(data, target_date_str, target_date_prev_str, date_col_name=date_col_name)

    @staticmethod
    def in_prev_n_month(data: DataFrame, n: int, date_col_name='date') -> DataFrame:
        curr_date = datetime.datetime.now()
        target_date = curr_date - relativedelta(months=n)
        target_date_str = target_date.strftime("%Y-%m-01")
        return AkshareDataset.prev_date(data, target_date_str, date_col_name=date_col_name)