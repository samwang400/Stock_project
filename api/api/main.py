import pandas as pd
from fastapi import FastAPI, HTTPException
import pymysql
from typing import Dict, List, Optional
from api.config import (
    MYSQL_DATA_USER,
    MYSQL_DATA_PASSWORD,
    MYSQL_DATA_HOST,
    MYSQL_DATA_PORT,
    MYSQL_DATA_DATABASE,
)

def get_mysql_financialdata_conn() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=MYSQL_DATA_HOST,   
        port=MYSQL_DATA_PORT,
        user=MYSQL_DATA_USER,
        password=MYSQL_DATA_PASSWORD,
        database=MYSQL_DATA_DATABASE
    )

app = FastAPI()

@app.get("/")
def read_root():
    return {"Stock": "Project"}

@app.get("/taiwan_stock_price")
def taiwan_stock_price(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    try:
        connection = get_mysql_financialdata_conn()
        
        with connection.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            sql = """
                SELECT * FROM taiwan_stock_price
                WHERE StockID = %s
                AND `Date` >= %s
                AND `Date` <= %s
            """
            # Execute query with parameters
            cursor.execute(sql, (stock_id, start_date, end_date))
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    # If error        
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()

@app.get("/taiwan_future_daily")
def taiwan_future_daily(future_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    try:
        connection = get_mysql_financialdata_conn()
        
        with connection.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            sql = """
                SELECT * FROM taiwan_future_daily
                WHERE FuturesID = %s
                AND `Date` >= %s
                AND `Date` <= %s
            """
            
            # Execute query with parameters
            cursor.execute(sql, (future_id, start_date, end_date))
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    # If error        
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()

@app.get("/taiwan_institutional_investor")
def taiwan_institutional_investor(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    try:
        connection = get_mysql_financialdata_conn()
        
        with connection.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            sql = """
                SELECT * FROM taiwan_institutional_investor
                WHERE StockID = %s
                AND `Date` >= %s
                AND `Date` <= %s
            """
            
            # Execute query with parameters
            cursor.execute(sql, (stock_id, start_date, end_date))
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    # If error        
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()

@app.get("/taiwan_margin_short_sale")
def taiwan_margin_short_sale(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    try:
        connection = get_mysql_financialdata_conn()
        
        with connection.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            sql = """
                SELECT * FROM taiwan_margin_short_sale
                WHERE StockID = %s
                AND `Date` >= %s
                AND `Date` <= %s
            """
            
            # Execute query with parameters
            cursor.execute(sql, (stock_id, start_date, end_date))
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    # If error        
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()

@app.get("/taiwan_share_holding")
def taiwan_share_holding(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    try:
        connection = get_mysql_financialdata_conn()
        
        with connection.cursor() as cursor:
            # Use parameterized query to prevent SQL injection
            sql = """
                SELECT * FROM taiwan_share_holding
                WHERE StockID = %s
                AND `Date` >= %s
                AND `Date` <= %s
            """
            
            # Execute query with parameters
            cursor.execute(sql, (stock_id, start_date, end_date))
            
            # Fetch results
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    # If error        
    except pymysql.Error as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()