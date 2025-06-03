import io
import typing
import pandas as pd
import time
import requests
from stockdata.schema.dataset import check_schema, TaiwanFuturesDaily

def futures_header():
    """
    Request header parameters, mimicking a browser request
    """

    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.taifex.com.tw",
        "Referer": "https://www.taifex.com.tw/cht/3/dlFutDailyMarketView",
    }

def crawler_futures(date: str) -> pd.DataFrame:
    """
    Crawl taifex data
    """

    url = "https://www.taifex.com.tw/cht/3/futDataDown"
    form_data = {
        "down_type": "1",
        "commodity_id": "all",
        "queryStartDate": date.replace("-", "/"),
        "queryEndDate": date.replace("-", "/"),
    }
    
    # To avoid being banned by taifex, sleep for 5 seconds before each request
    time.sleep(5)
    resp = requests.post(url, headers=futures_header(), data=form_data)
    
    if resp.ok and resp.content: # if HTTP 200 and not empty
        
        # Load data to DataFrame
        df = pd.read_csv(
            io.StringIO(resp.content.decode("big5")), index_col=False
        )
        return df
    
    else:
        return pd.DataFrame()
    

def colname_zh2en(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Chinese column names to English
    """
    colname_dict = {
        "交易日期": "date",
        "契約": "FuturesID",
        "到期月份(週別)": "ContractDate",
        "開盤價": "Open",
        "最高價": "Max",
        "最低價": "Min",
        "收盤價": "Close",
        "漲跌價": "Change",
        "漲跌%": "ChangePer",
        "成交量": "Volume",
        "結算價": "SettlementPrice",
        "未沖銷契約數": "OpenInterest",
        "交易時段": "TradingSession",
    }
    df = df.drop(
        ["最後最佳買價", "最後最佳賣價", "歷史最高價", "歷史最低價", "是否因訊息面暫停交易", "價差對單式委託成交量"], axis=1
    )
    
    df.columns = [colname_dict[col] for col in df.columns]

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean data
    """

    df["date"] = df["date"].str.replace("/", "-")
    df["ChangePer"] = df["ChangePer"].str.replace("%", "")
    df["ContractDate"] = df["ContractDate"].astype(str).str.replace(" ", "")
    
    # Convert TradingSession to English
    if "TradingSession" in df.columns:
        df["TradingSession"] = df["TradingSession"].map({"一般": "Position", "盤後": "AfterMarket"})
    
    else:
        df["TradingSession"] = "Position"
    
    # Convert - to 0
    for col in ["Open", "Max", "Min", "Close", "Change", "ChangePer", "Volume", "SettlementPrice", "OpenInterest"]:
        df[col] = df[col].replace("-", "0").astype(float)
    
    # Fill NA to 0
    df = df.fillna(0)
    
    return df


def future_pipeline(date: str) -> pd.DataFrame:
    """
    Crawl pipeline
    """
    
    
    print(f"Start_crawl_taifex_{date}_data...")
    df = crawler_futures(date)

    df = colname_zh2en(df.copy())
    print(f"Start_clean_taifex_{date}_data...")
    df = clean_data(df.copy())
            
    df["Date"] = date
    
    # Check columns type
    df = check_schema(df.copy(), TaiwanFuturesDaily)
   
    return df
