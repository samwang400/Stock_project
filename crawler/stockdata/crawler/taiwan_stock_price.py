import typing
import pandas as pd
import requests
import time
from stockdata.schema.dataset import check_schema, TaiwanStockPrice
from typing import Tuple



def twse_header():
    """
    Request header parameters, mimicking a browser request
    """

    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Host": "www.twse.com.tw",
        "Referer": "https://www.twse.com.tw/zh/page/trading/exchange/MI_INDEX.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

def colname_zh2en(df: pd.DataFrame, colname: typing.List[str],) -> pd.DataFrame:
    """
    Convert Chinese column names to English
    """

    taiwan_stock_price = {
        "證券代號": "StockID",
        "證券名稱": "",
        "成交股數": "TradeVolume",
        "成交筆數": "Transaction",
        "成交金額": "TradeValue",
        "開盤價": "Open",
        "最高價": "Max",
        "最低價": "Min",
        "收盤價": "Close",
        "漲跌(+/-)": "Dir",
        "漲跌價差": "Change",
        "最後揭示買價": "",
        "最後揭示買量": "",
        "最後揭示賣價": "",
        "最後揭示賣量": "",
        "本益比": "",
    }
    
    df.columns = [taiwan_stock_price[col] for col in colname]
    df = df.drop([""], axis=1)
    
    return df

def clear_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clear data
    """

    for col in ["TradeVolume", "Transaction", "TradeValue", "Open", "Max", "Min", "Close", "Change"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "")
            .str.replace("X", "")
            .str.replace("+", "")
            .str.replace("----", "0")
            .str.replace("---", "0")
            .str.replace("--", "0")
            .str.replace(" ", "")
            .str.replace("除權息", "0")
            .str.replace("除息", "0")
            .str.replace("除權", "0")
        )
    
    return df

def convert_change(df: pd.DataFrame,) -> pd.DataFrame:
    """
    Convert stock price change direction and amount into a single numeric column.
    """
    
    df["Dir"] = (
        df["Dir"]
        .str.split(">")
        .str[1]
        .str.split("<")
        .str[0]
    )

    df["Change"] = (df["Dir"] + df["Change"])
    
    df["Change"] = (
        df["Change"]
        .str.replace(" ", "")
        .str.replace("X", "")
        .astype(float)
    )

    df = df.fillna("")
    df = df.drop(["Dir"], axis=1)
    
    return df


def crawler_twse(date: str,) -> pd.DataFrame:
    """
    Crawl twse data
    """

    # Request url
    url = ("https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date}&type=ALL")
    url = url.format(date=date.replace("-", ""))
    
    # To avoid being banned by twse, sleep for 5 seconds before each request
    time.sleep(5)
    res = requests.get(url, headers=twse_header())

    if (res.json()["stat"] == "很抱歉，沒有符合條件的資料!"):
        return pd.DataFrame()
    
    # After 2009, stock prices are in "data9" in the response  
    # Before 2009, stock prices are in "data8" in the response  
    # Consider cases where no data is available, such as no trading on Saturdays now, but trading was available on Saturdays in 2007  
    try:
        if "data9" in res.json():
            df = pd.DataFrame(res.json()["data9"])
            colname = res.json()["fields9"]
        
        elif "data8" in res.json():
            df = pd.DataFrame(res.json()["data8"])
            colname = res.json()["fields8"]
        
        elif res.json()["stat"] in ["查詢日期小於93年2月11日，請重新查詢!", "很抱歉，沒有符合條件的資料!"]:
            return pd.DataFrame()
        
        else:
            tables = res.json().get("tables", [{}])
            df = pd.DataFrame(tables[8]["data"])
            colname = tables[8]["fields"]

    except BaseException:
        return pd.DataFrame()

    if len(df) == 0:
        return pd.DataFrame()
    
    # Convert column names from Chinese to English
    df = colname_zh2en(df.copy(), colname)
    df["Date"] = date
    df = clear_data(df.copy())
    
    # Convert Dir to number(+-)
    df = convert_change(df)
    
    return df


def tpex_header():
    """
    Request header parameters, mimicking a browser request
    """

    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Host": "www.tpex.org.tw",
        "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


def set_column(df: pd.DataFrame,) -> pd.DataFrame:
    """
    Set columns name
    """

    df.columns = [
        "StockID",
        "Close",
        "Change",
        "Open",
        "Max",
        "Min",
        "TradeVolume",
        "TradeValue",
        "Transaction",
    ]
    
    return df

def convert_date(date: str) -> str:
    """
    Convert date to correct formation
    """

    year, month, day = date.split("-")
    year = int(year) - 1911
    
    return f"{year}{month}{day}"

def crawler_tpex(date: str,) -> pd.DataFrame:
    """
    Crawl tpex data
    """
    
    # Request url
    url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={date}&se=AL"
    url = url.format(date=convert_date(date))
    
    # To avoid being banned by TPEX, sleep for 5 seconds before each request
    time.sleep(5)
    res = requests.get(url, headers=tpex_header())
    data = res.json().get("tables", [])[0].get("data", [])
    df = pd.DataFrame(data)

    if not data or len(df) == 0:
        return pd.DataFrame()
    
    # The data returned by TPEX does not include column names, so specific columns are accessed by index
    df = df[[0, 2, 3, 4, 5, 6, 7, 8, 9]]
    
    # Convert column names from Chinese to English
    df = set_column(df.copy())
    df["Date"] = date
    df = clear_data(df.copy())
    
    # Select and reorder the relevant columns
    df = df[[
        "StockID",
        "TradeVolume",
        "Transaction",
        "TradeValue",
        "Open",
        "Max",
        "Min",
        "Close",
        "Change",
        "Date"
        ]]

    return df

def stock_price_pipeline(date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    

    print(f"Start_crawl_twse_{date}_data...")
    df_twse = crawler_twse(date)

    print(f"Start_crawl_tpex_{date}_data...")
    df_tpex = crawler_tpex(date)
    
    df_twse = check_schema(df_twse.copy(), TaiwanStockPrice)
    df_tpex = check_schema(df_tpex.copy(), TaiwanStockPrice)
    
    return df_twse, df_tpex
