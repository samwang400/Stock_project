import os

MYSQL_DATA_HOST = os.environ.get("MYSQL_DATA_HOST", "127.0.0.1")
MYSQL_DATA_USER = os.environ.get("MYSQL_DATA_USER", "root")
MYSQL_DATA_PASSWORD = os.environ.get("MYSQL_DATA_PASSWORD", "test")
MYSQL_DATA_PORT = int(os.environ.get("MYSQL_DATA_PORT", "3306"))
MYSQL_DATA_DATABASE = os.environ.get("MYSQL_DATA_DATABASE", "stockdata")
