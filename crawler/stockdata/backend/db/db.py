import typing
from typing import List
import pandas as pd
import pymysql
from loguru import logger


def update2mysql_by_sql(df: pd.DataFrame, table: str, mysql_conn):
    """
    Upload DataFrame to MySQL using SQL INSERT
    """

    if len(df) > 0:
        try:
            
            # Create a cursor
            with mysql_conn.cursor() as cursor:
                
                colname = ",".join(f'`{col}`' for col in df.columns)
                sql = f"INSERT INTO {table} ({colname}) VALUES ({', '.join(['%s'] * len(df.columns))})"
                
                # Insert the data
                records = df.to_records(index=False).tolist()
                cursor.executemany(sql, records)
                
                # Commit the transaction
                mysql_conn.commit()
            
            return True
        
        except Exception as e:
            
            logger.error(f"MySQL insertion error: {type(e).__name__}: {e}")
            mysql_conn.rollback()
           
            return False
    
    return True

def update2mysql_by_sql_for_info(df: pd.DataFrame, table: str, mysql_conn):
    """
    Upload DataFrame to MySQL using SQL INSERT
    """

    if len(df) > 0:
        try:
            
            # Create a cursor
            with mysql_conn.cursor() as cursor:
                
                colname = ",".join(f'`{col}`' for col in df.columns)
                update_cols = ['StockName', 'MarketType']
                sql = f"INSERT INTO {table} ({colname}) VALUES ({', '.join(['%s']*len(df.columns))}) ON DUPLICATE KEY UPDATE {', '.join([f'{c}=VALUES({c})' for c in update_cols])}"

                
                # Insert the data
                records = df.to_records(index=False).tolist()
                cursor.executemany(sql, records)
                
                # Commit the transaction
                mysql_conn.commit()
            
            return True
        
        except Exception as e:
            
            logger.error(f"MySQL insertion error: {type(e).__name__}: {e}")
            mysql_conn.rollback()
           
            return False
    
    return True