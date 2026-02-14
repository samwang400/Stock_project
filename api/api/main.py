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

# Import Prometheus instrumentation
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram, Gauge
import time

def get_mysql_financialdata_conn() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=MYSQL_DATA_HOST,   
        port=MYSQL_DATA_PORT,
        user=MYSQL_DATA_USER,
        password=MYSQL_DATA_PASSWORD,
        database=MYSQL_DATA_DATABASE
    )

app = FastAPI()

# Initialize Prometheus metrics (automatic HTTP metrics)
Instrumentator().instrument(app).expose(app)

# Custom business metrics
# Counter: Total API queries by endpoint and stock/future ID
api_queries_counter = Counter(
    'stock_api_queries_total',
    'Total stock API queries',
    ['endpoint', 'id_type', 'id_value']
)

# Histogram: Database query duration
db_query_duration = Histogram(
    'stock_api_db_query_duration_seconds',
    'Database query duration',
    ['table_name']
)

# Counter: Database errors
db_errors_counter = Counter(
    'stock_api_db_errors_total',
    'Total database errors',
    ['endpoint', 'error_type']
)

# Gauge: Active database connections
active_db_connections = Gauge(
    'stock_api_active_db_connections',
    'Current active database connections'
)

@app.get("/")
def read_root():
    return {"Stock": "Project"}

@app.get("/taiwan_stock_price")
def taiwan_stock_price(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    # Record query
    api_queries_counter.labels(
        endpoint='taiwan_stock_price',
        id_type='stock',
        id_value=stock_id
    ).inc()
    
    try:
        connection = get_mysql_financialdata_conn()
        active_db_connections.inc()  # Increment active connections
        
        with connection.cursor() as cursor:
            # Measure database query time
            start_time = time.time()
            
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
            
            # Record query duration
            query_duration = time.time() - start_time
            db_query_duration.labels(table_name='taiwan_stock_price').observe(query_duration)
            
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    
    # If error        
    except pymysql.Error as e:
        # Record database error
        db_errors_counter.labels(
            endpoint='taiwan_stock_price',
            error_type=type(e).__name__
        ).inc()
        
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()
            active_db_connections.dec()  # Decrement active connections

@app.get("/taiwan_future_daily")
def taiwan_future_daily(future_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    # Record query
    api_queries_counter.labels(
        endpoint='taiwan_future_daily',
        id_type='future',
        id_value=future_id
    ).inc()
    
    try:
        connection = get_mysql_financialdata_conn()
        active_db_connections.inc()
        
        with connection.cursor() as cursor:
            # Measure database query time
            start_time = time.time()
            
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
            
            # Record query duration
            query_duration = time.time() - start_time
            db_query_duration.labels(table_name='taiwan_future_daily').observe(query_duration)
            
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    
    # If error        
    except pymysql.Error as e:
        # Record database error
        db_errors_counter.labels(
            endpoint='taiwan_future_daily',
            error_type=type(e).__name__
        ).inc()
        
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()
            active_db_connections.dec()

@app.get("/taiwan_institutional_investor")
def taiwan_institutional_investor(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    # Record query
    api_queries_counter.labels(
        endpoint='taiwan_institutional_investor',
        id_type='stock',
        id_value=stock_id
    ).inc()
    
    try:
        connection = get_mysql_financialdata_conn()
        active_db_connections.inc()
        
        with connection.cursor() as cursor:
            # Measure database query time
            start_time = time.time()
            
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
            
            # Record query duration
            query_duration = time.time() - start_time
            db_query_duration.labels(table_name='taiwan_institutional_investor').observe(query_duration)
            
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    
    # If error        
    except pymysql.Error as e:
        # Record database error
        db_errors_counter.labels(
            endpoint='taiwan_institutional_investor',
            error_type=type(e).__name__
        ).inc()
        
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()
            active_db_connections.dec()

@app.get("/taiwan_margin_short_sale")
def taiwan_margin_short_sale(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    # Record query
    api_queries_counter.labels(
        endpoint='taiwan_margin_short_sale',
        id_type='stock',
        id_value=stock_id
    ).inc()
    
    try:
        connection = get_mysql_financialdata_conn()
        active_db_connections.inc()
        
        with connection.cursor() as cursor:
            # Measure database query time
            start_time = time.time()
            
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
            
            # Record query duration
            query_duration = time.time() - start_time
            db_query_duration.labels(table_name='taiwan_margin_short_sale').observe(query_duration)
            
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    
    # If error        
    except pymysql.Error as e:
        # Record database error
        db_errors_counter.labels(
            endpoint='taiwan_margin_short_sale',
            error_type=type(e).__name__
        ).inc()
        
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()
            active_db_connections.dec()

@app.get("/taiwan_share_holding")
def taiwan_share_holding(stock_id: str = '', start_date: str = '', end_date: str = '') -> Dict[str, List[Dict]]:
    # Record query
    api_queries_counter.labels(
        endpoint='taiwan_share_holding',
        id_type='stock',
        id_value=stock_id
    ).inc()
    
    try:
        connection = get_mysql_financialdata_conn()
        active_db_connections.inc()
        
        with connection.cursor() as cursor:
            # Measure database query time
            start_time = time.time()
            
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
            
            # Record query duration
            query_duration = time.time() - start_time
            db_query_duration.labels(table_name='taiwan_share_holding').observe(query_duration)
            
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            # Convert datetime to string
            df['Date'] = df['Date'].astype(str)
            data = df.to_dict(orient='records')
            return {"data": data}
    
    # If error        
    except pymysql.Error as e:
        # Record database error
        db_errors_counter.labels(
            endpoint='taiwan_share_holding',
            error_type=type(e).__name__
        ).inc()
        
        raise HTTPException(
            status_code=500,
            detail=f"Database error occurred: {str(e)}"
        )
    
    # Close connection
    finally:
        if 'connection' in locals():
            connection.close()
            active_db_connections.dec()