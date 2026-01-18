import typing
import pandas as pd
import requests
import time
from stockdata.schema.dataset import check_schema, TaiwanStockInfo
from typing import Tuple


def colname_zh2en(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Chinese column names to English
    """
    
    column_mapping = {
        "市場別": "MarketType",
        "產業別": "IndustryType",
    }
    
    df = df.rename(columns=column_mapping)
    
    return df


def split_stock_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split stock code and name from combined column
    """
    
    df[['StockID', 'StockName']] = (
        df['有價證券代號及名稱']
        .astype(str)
        .str.strip()
        .str.split(r'\s+', n=1, expand=True)
    )
    
    return df


def filter_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove invalid header-like rows and non-numeric stock IDs
    """
    
    # Remove rows where all fields contain the same text
    df = df[~(
        (df['有價證券代號及名稱'] == df['MarketType']) &
        (df['MarketType'] == df['IndustryType'])
    )]
    
    # Drop rows with invalid security IDs (non-numeric)
    df = df[df['StockID'].str.match(r'^\d+$', na=False)]
    
    return df


def crawler_tpex_isin() -> pd.DataFrame:
    """
    Crawl TWSE ISIN data
    """
    
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
    resp = requests.get(url)
    resp.encoding = 'big5'
    
    # Read HTML table
    df = pd.read_html(resp.text)[0]
    
    # Set correct column names (first row is header)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    
    # Keep only required columns
    df = df[['有價證券代號及名稱', '市場別', '產業別']].copy()
    
    # Convert column names from Chinese to English
    df = colname_zh2en(df.copy())
    df = split_stock_info(df.copy())
    df = filter_invalid_rows(df.copy())
    
    # Final column order
    df = df[['StockID', 'StockName', 'MarketType', 'IndustryType']].reset_index(drop=True)
    df['MarketType'] = 'tpex'
    df['IndustryType'] = df['IndustryType'].fillna('未知')

    
    return df

def crawler_twse_isin() -> pd.DataFrame:
    """
    Crawl TWSE ISIN data
    """
    
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    resp = requests.get(url)
    resp.encoding = 'big5'
    
    # Read HTML table
    df = pd.read_html(resp.text)[0]
    
    # Set correct column names (first row is header)
    df.columns = df.iloc[0]
    df = df.iloc[1:].reset_index(drop=True)
    
    # Keep only required columns
    df = df[['有價證券代號及名稱', '市場別', '產業別']].copy()
    
    # Convert column names from Chinese to English
    df = colname_zh2en(df.copy())
    df = split_stock_info(df.copy())
    df = filter_invalid_rows(df.copy())
    
    # Final column order
    df = df[['StockID', 'StockName', 'MarketType', 'IndustryType']].reset_index(drop=True)
    df['MarketType'] = 'twse'
    df['IndustryType'] = df['IndustryType'].fillna('未知')

    
    return df

def stock_info_pipeline() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Crawl pipeline
    """

    print(f"Start_crawl_twse_info...")
    df_twse = crawler_twse_isin()

    print(f"Start_crawl_tpex_info...")
    df_tpex = crawler_tpex_isin()
    
    df_twse = check_schema(df_twse.copy(), TaiwanStockInfo)
    df_tpex = check_schema(df_tpex.copy(), TaiwanStockInfo)
    
    return df_twse, df_tpex
