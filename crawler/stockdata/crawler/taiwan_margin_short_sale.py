import typing
import pandas as pd
import requests
import time
from typing import Tuple
from stockdata.schema.dataset import check_schema, TaiwanMarginPurchaseShortSale
from typing import Type


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
        "Referer": "https://www.twse.com.tw/zh/page/trading/exchange/MI_MARGN.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


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
        "Referer": "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal.php?l=zh-tw",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


def clear_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clear data - remove commas and convert to numeric
    """
    for col in df.columns:
        if col not in ["StockID", "StockName", "Note", "Date"]:
            df[col] = df[col].replace(",", "", regex=True) \
                             .replace("--", "0") \
                             .replace("", "0")
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    
    return df


def convert_date_to_roc(date: str) -> str:
    """
    Convert date to ROC (Republic of China) calendar format
    """
    date_obj = pd.to_datetime(date)
    roc_year = date_obj.year - 1911
    
    return f"{roc_year}/{date_obj.month:02d}/{date_obj.day:02d}"


def crawler_twse(date: str) -> pd.DataFrame:
    """
    Crawl TWSE margin purchase and short sale data
    """
    url = "https://www.twse.com.tw/exchangeReport/MI_MARGN"
    
    params = {
        "date": date.replace("-", ""),
        "selectType": "ALL",
        "response": "json"
    }
    
    # To avoid being banned by TWSE, sleep for 5 seconds before each request
    time.sleep(5)
    
    res = requests.get(url, params=params, headers=twse_header(), timeout=30)
    res.raise_for_status()
    data = res.json()
    
    if data.get("stat") == "很抱歉，沒有符合條件的資料":
        return pd.DataFrame()
    
    
    df = pd.DataFrame(data["tables"][1]["data"], columns=data["tables"][1]["fields"])
    
    if len(df) == 0:
        return pd.DataFrame()
    
    df["Date"] = date

    df.columns = [
        "StockID",
        "StockName",
        "MarginPurchaseBuy",
        "MarginPurchaseSell",
        "MarginPurchaseCashRepayment",
        "MarginPurchaseYesterdayBalance",
        "MarginPurchaseTodayBalance",
        "MarginPurchaseLimit",
        "ShortSaleBuy",
        "ShortSaleSell",
        "ShortSaleCashRepayment",
        "ShortSaleYesterdayBalance",
        "ShortSaleTodayBalance",
        "ShortSaleLimit",
        "OffsetLoanAndShort",
        "Note",
        "Date"
    ]
    
    df = clear_data(df.copy())
    
    return df


def set_tpex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set TPEX column names
    """
    df = df[[
        "代號",             
        "名稱",             
        "資買",             
        "資賣",             
        "現償",             
        "前資餘額(張)",     
        "資餘額",           
        "資限額",           
        "券買",             
        "券賣",             
        "券償",             
        "前券餘額(張)",     
        "券餘額",           
        "券限額",           
        "資券相抵(張)",   
        "備註"
        ]]
    

    
    return df


def crawler_tpex(date: str) -> pd.DataFrame:
    """
    Crawl TPEX margin purchase and short sale data
    """
    url = "https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php"
    
    params = {
        "l": "zh-tw",
        "o": "json",
        "d": convert_date_to_roc(date),
        "t": "D"
    }
    
    # To avoid being banned by TPEX, sleep for 5 seconds before each request
    time.sleep(5)
    
    res = requests.get(url, params=params, headers=tpex_header(), timeout=30)
    res.raise_for_status()
    data = res.json()
     
    df = pd.DataFrame(data["tables"][0]["data"], columns=data["tables"][0]["fields"])
    
    # Set column names
    df = set_tpex_columns(df.copy())

    df["Date"] = date
    
    df.columns = [
        "StockID",
        "StockName",
        "MarginPurchaseBuy",
        "MarginPurchaseSell",
        "MarginPurchaseCashRepayment",
        "MarginPurchaseYesterdayBalance",
        "MarginPurchaseTodayBalance",
        "MarginPurchaseLimit",
        "ShortSaleBuy",
        "ShortSaleSell",
        "ShortSaleCashRepayment",
        "ShortSaleYesterdayBalance",
        "ShortSaleTodayBalance",
        "ShortSaleLimit",
        "OffsetLoanAndShort",
        "Note",
        "Date"
    ]

    df = clear_data(df.copy())
     
    return df


def margin_short_sale_pipeline(date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main pipeline to crawl margin purchase and short sale data from both TWSE and TPEX
    
    Args:
        date: Date in format 'YYYY-MM-DD'
    
    Returns:
        Tuple of (df_twse, df_tpex)
    """
    print(f"Start_crawl_twse_margin_short_{date}_data...")
    df_twse = crawler_twse(date)
    
    print(f"Start_crawl_tpex_margin_short_{date}_data...")
    df_tpex = crawler_tpex(date)
    
    df_twse = check_schema(df_twse.copy(), TaiwanMarginPurchaseShortSale)
    df_tpex = check_schema(df_tpex.copy(), TaiwanMarginPurchaseShortSale)
    
    return df_twse, df_tpex