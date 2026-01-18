import typing
import pandas as pd
import requests
import time
from typing import Tuple
from stockdata.schema.dataset import check_schema, TaiwanInstitutionalInvestor

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
        "Referer": "https://www.twse.com.tw/fund/T86",
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
        "Referer": "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge.php?l=zh-tw",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


def colname_zh2en(df: pd.DataFrame, colname: typing.List[str]) -> pd.DataFrame:
    """
    Convert Chinese column names to English
    """
    institutional_investors = {
        "證券代號": "StockID",
        "證券名稱": "StockName",
        "外陸資買進股數(不含外資自營商)": "ForeignBuy",
        "外陸資賣出股數(不含外資自營商)": "ForeignSell",
        "外陸資買賣超股數(不含外資自營商)": "ForeignNet",
        "外資自營商買進股數": "ForeignDealerBuy",
        "外資自營商賣出股數": "ForeignDealerSell",
        "外資自營商買賣超股數": "ForeignDealerNet",
        "投信買進股數": "InvestmentTrustBuy",
        "投信賣出股數": "InvestmentTrustSell",
        "投信買賣超股數": "InvestmentTrustNet",
        "自營商買賣超股數": "DealerNet",
        "自營商買進股數(自行買賣)": "DealerSelfBuy",
        "自營商賣出股數(自行買賣)": "DealerSelfSell",
        "自營商買賣超股數(自行買賣)": "DealerSelfNet",
        "自營商買進股數(避險)": "DealerHedgeBuy",
        "自營商賣出股數(避險)": "DealerHedgeSell",
        "自營商買賣超股數(避險)": "DealerHedgeNet",
        "三大法人買賣超股數": "ThreeInstitutionNet",
    }
    
    df.columns = [institutional_investors.get(col, col) for col in colname]
    
    return df


def clear_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clear data - remove commas and convert to numeric
    """
    df = df.replace(",", "", regex=True)
    
    for col in df.columns:
        if col not in ["StockID", "StockName", "Date"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
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
    Crawl TWSE institutional investors data
    """
    url = "https://www.twse.com.tw/fund/T86"
    
    params = {
        "date": date.replace("-", ""),
        "selectType": "ALLBUT0999",
        "response": "json"
    }
    
    # To avoid being banned by TWSE, sleep for 5 seconds before each request
    time.sleep(5)
    
    try:
        res = requests.get(url, params=params, headers=twse_header(), timeout=30)
        res.raise_for_status()
        data = res.json()
        
        if data.get("stat") == "很抱歉，沒有符合條件的資料!":
            return pd.DataFrame()
        
        if "data" not in data or not data["data"]:
            return pd.DataFrame()
        
        df = pd.DataFrame(data["data"], columns=data["fields"])
        
        if len(df) == 0:
            return pd.DataFrame()
        
        # Convert column names from Chinese to English
        df = colname_zh2en(df.copy(), data["fields"])
        df = clear_data(df.copy())
        df["Date"] = date
        
        return df
        
    except Exception as e:
        print(f"Error crawling TWSE data for {date}: {e}")
        return pd.DataFrame()


def set_tpex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set TPEX column names (positions: 0,1,2,3,4,5,6,7,11,12,13,14,15,16,17,18,19,23)
    """
    df.columns = [
        "StockID",
        "StockName",
        "ForeignBuy",
        "ForeignSell",
        "ForeignNet",
        "ForeignDealerBuy",
        "ForeignDealerSell",
        "ForeignDealerNet",
        "InvestmentTrustBuy",
        "InvestmentTrustSell",
        "InvestmentTrustNet",
        "DealerSelfBuy",
        "DealerSelfSell",
        "DealerSelfNet",
        "DealerHedgeBuy",
        "DealerHedgeSell",
        "DealerHedgeNet",
        "ThreeInstitutionNet",
    ]
    
    return df


def crawler_tpex(date: str) -> pd.DataFrame:
    """
    Crawl TPEX institutional investors data
    """
    url = "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
    
    params = {
        "l": "zh-tw",
        "t": "D",
        "d": convert_date_to_roc(date),
        "o": "json"
    }
    
    # To avoid being banned by TPEX, sleep for 5 seconds before each request
    time.sleep(5)
    
    try:
        res = requests.get(url, params=params, headers=tpex_header(), timeout=30)
        res.raise_for_status()
        data = res.json()
        
        tables = data.get("tables", [])
        if not tables or len(tables) == 0:
            return pd.DataFrame()
        
        table_data = tables[0].get("data", [])
        if not table_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(table_data)
        
        # Keep specific columns by position
        keep_cols = [0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 16, 17, 18, 19, 23]
        df = df.iloc[:, keep_cols].copy()
        
        # Set column names
        df = set_tpex_columns(df.copy())
        df = clear_data(df.copy())
        df["Date"] = date
        
        return df
        
    except Exception as e:
        print(f"Error crawling TPEX data for {date}: {e}")
        return pd.DataFrame()


def institutional_investor_pipeline(date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main pipeline to crawl institutional investors data from both TWSE and TPEX
    
    Args:
        date: Date in format 'YYYY-MM-DD'
    
    Returns:
        Tuple of (df_twse, df_tpex)
    """
    print(f"Start_crawl_twse_institutional_{date}_data...")
    df_twse = crawler_twse(date)
    
    print(f"Start_crawl_tpex_institutional_{date}_data...")
    df_tpex = crawler_tpex(date)
    
    df_twse = check_schema(df_twse.copy(), TaiwanInstitutionalInvestor)
    df_tpex = check_schema(df_tpex.copy(), TaiwanInstitutionalInvestor)
    
    return df_twse, df_tpex