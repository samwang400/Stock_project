import pymysql
from typing import Any

from stockdata.config import (
    MYSQL_DATA_USER,
    MYSQL_DATA_PASSWORD,
    MYSQL_DATA_HOST,
    MYSQL_DATA_PORT,
    MYSQL_DATA_DATABASE,
)


def get_mysql_stockdata_conn() -> Any:
    """
    Get MySQL stockdata database connetcion
    
    Returns:
        pymysql.connections.Connection: database connection object
        
    Connection informations:
    - user: root
    - password: test
    - host: localhost
    - port: 3306
    - database: stockdata
    """

    try:
        connection = pymysql.connect(
            host= MYSQL_DATA_HOST,
            port= MYSQL_DATA_PORT,
            user= MYSQL_DATA_USER ,
            password= MYSQL_DATA_PASSWORD,
            database= MYSQL_DATA_DATABASE,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    
    except Exception as e:
        print(f"Error exists while connecting database: {str(e)}")
        raise
