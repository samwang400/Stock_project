import time
import typing
from loguru import logger
import pymysql

from pymysql.connections import Connection
from .clients import get_mysql_stockdata_conn


def check_alive(connect: Connection) -> None:
    """
    Check if the connection is alive
    
    Args:
        connect: PyMySQL object
    """
    with connect.cursor() as cursor:
        cursor.execute("SELECT 1 + 1")

def reconnect(connect_func: typing.Callable[[], Connection]) -> Connection:
    """
    Reconnecting
    
    Args:
        connect_func: get_mysql_stockdata_conn (client) 
    
    Returns:
        Connection: new connection
    """
    try:
        connect = connect_func()
        return connect
    
    except Exception as e:
        logger.info(
            f"{connect_func.__name__} reconnect error {e}"
        )
        return None

def check_connect_alive(connect: Connection,connect_func: typing.Callable[[], Connection]) -> Connection:
    """
    Checking connection status and reconnecting when necessary
    
    Args:
        connect: Current database connection
        connect_func: get_mysql_stockdata_conn (client)
    
    Returns:
        Connection: Valid database connection
    """
    
    if connect:
        try:
            check_alive(connect)
            return connect
        
        except Exception as e:
            logger.info(f"{connect_func.__name__} connect, error: {e}")
            
            try:
                connect.close()
            
            except:
                pass
            
            time.sleep(1)
            connect = reconnect(connect_func)
            
            return check_connect_alive(connect, connect_func)
    else:
        connect = reconnect(connect_func)
        
        return check_connect_alive(connect, connect_func)

class Router:
    def __init__(self):
        """
        Initialize Router
        """
        
        self._mysql_stockdata_conn = (get_mysql_stockdata_conn())
    
    def check_mysql_stockdata_conn_alive(self) -> Connection:
        """
        Check connection status
        """

        self._mysql_stockdata_conn = check_connect_alive(self._mysql_stockdata_conn, get_mysql_stockdata_conn)
        
        return self._mysql_stockdata_conn
    
    @property # Convert method to attribute
    def mysql_stockdata_conn(self) -> Connection:
        """
        Use a property to ensure that every time a connection is accessed, 
        it first goes through a "check alive" process to verify if the connection is active.
        """

        return self.check_mysql_stockdata_conn_alive()
    
    def close_connection(self):
        """
        Close connection
        """
        self._mysql_stockdata_conn.close()