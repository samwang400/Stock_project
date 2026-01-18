import typing
import pandas as pd
import requests
import time
from io import StringIO
from typing import Optional
from stockdata.schema.dataset import check_schema, TDCCShareholding


def colname_zh2en(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Chinese column names to English
    """
    
    column_mapping = {
        "資料日期": "Date",
        "證券代號": "StockID",
        "持股分級": "ShareholdingLevel",
        "人數": "NumberOfHolders",
        "股數": "NumberOfShares",
        "占集保庫存數比例%": "PercentageOfTotalShares"
    }
    
    df = df.rename(columns=column_mapping)
    
    return df


def format_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert date format from YYYYMMDD to YYYY-MM-DD
    """
    
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    
    return df


def clean_security_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove leading zeros from security codes
    """
    
    df['StockID'] = df['StockID'].astype(str).str.lstrip('0')
    
    return df


def crawler_tdcc_shareholding() -> pd.DataFrame:
    """
    Crawl TDCC (Taiwan Depository & Clearing Corporation) shareholding distribution data
    Downloads CSV containing all stocks' shareholding data for the latest period
    """
    
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # TDCC CSV uses Big5 encoding
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    # Parse CSV data
    df = pd.read_csv(StringIO(response.text))
    
    return df


def share_holding_pipeline() -> pd.DataFrame:
    """
    TDCC shareholding data crawling pipeline
    """
    
    print(f"Start_crawl_share_holding_data...")
    
    # Download raw data
    df = crawler_tdcc_shareholding()
    
    # Data transformation pipeline
    df = colname_zh2en(df.copy())
    df = format_date(df.copy())
    df = clean_security_code(df.copy())
    
    df = check_schema(df.copy(), TDCCShareholding)
    
    return df