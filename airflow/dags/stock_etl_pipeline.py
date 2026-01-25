from datetime import datetime, timedelta
import time
import os
import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator


# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# Helper function
def sleep_3_seconds():
    """
    Simple delay to avoid hitting rate limits / being banned by the website.
    Each task needs its own PythonOperator instance.
    """
    time.sleep(3)

taipei_tz = pendulum.timezone("Asia/Taipei")

# DAG definition
with DAG(
    dag_id="taiwan_stock_data_crawler",
    description="Crawl Taiwan stock market data daily",
    start_date=datetime(2025, 1, 1, tzinfo=taipei_tz),
    schedule="0 21 * * 1-7",  # Every weekday at 18:00
    catchup=False,
    max_active_runs=1,
    tags=["stock", "taiwan", "crawler"],
    default_args=default_args,
) as dag:


    # Docker configuration
    docker_config = {
        "image": "stockdata_crawler:latest",
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": "dev",
        "auto_remove": "never",
        "environment": {
        "MYSQL_DATA_HOST": os.getenv("MYSQL_DATA_HOST", "mysql"),
        "MYSQL_DATA_USER": os.getenv("MYSQL_DATA_USER", "root"),
        "MYSQL_DATA_PASSWORD": os.getenv("MYSQL_DATA_PASSWORD", "test"),
        "MYSQL_DATA_PORT": os.getenv("MYSQL_DATA_PORT", "3306"),
        "MYSQL_DATA_DATABASE": os.getenv("MYSQL_DATA_DATABASE", "stockdata"),
      }
    }

    # Define tasks
    # Update stock info
    task_stock_info = DockerOperator(
        task_id="update_stock_info",
        command="python -m stockdata.main taiwan_stock_info",
        **docker_config,
    )
    delay_after_stock_info = PythonOperator(
        task_id="delay_after_stock_info",
        python_callable=sleep_3_seconds,
    )

    # Update share holding
    task_share_holding = DockerOperator(
        task_id="update_share_holding",
        command="python -m stockdata.main taiwan_share_holding",
        **docker_config,
    )
    delay_after_share_holding = PythonOperator(
        task_id="delay_after_share_holding",
        python_callable=sleep_3_seconds,
    )

    # Update stock price
    task_stock_price = DockerOperator(
        task_id="update_stock_price",
        command="python -m stockdata.main taiwan_stock_price {{ ds }} {{ ds }}",
        **docker_config,
    )
    delay_after_stock_price = PythonOperator(
        task_id="delay_after_stock_price",
        python_callable=sleep_3_seconds,
    )

    # Update institutional investor
    task_institutional_investor = DockerOperator(
        task_id="update_institutional_investor",
        command="python -m stockdata.main taiwan_institutional_investor {{ ds }} {{ ds }}",
        **docker_config,
    )
    delay_after_institutional = PythonOperator(
        task_id="delay_after_institutional",
        python_callable=sleep_3_seconds,
    )

    # Update margin short sale
    task_margin_short_sale = DockerOperator(
        task_id="update_margin_short_sale",
        command="python -m stockdata.main taiwan_margin_short_sale {{ ds }} {{ ds }}",
        **docker_config,
    )
    delay_after_margin = PythonOperator(
        task_id="delay_after_margin",
        python_callable=sleep_3_seconds,
    )

    # Update future daily
    task_future_daily = DockerOperator(
        task_id="update_future_daily",
        command="python -m stockdata.main taiwan_future_daily {{ ds }} {{ ds }}",
        **docker_config,
    )

    (
        task_stock_info
        >> delay_after_stock_info
        >> task_share_holding
        >> delay_after_share_holding
        >> task_stock_price
        >> delay_after_stock_price
        >> task_institutional_investor
        >> delay_after_institutional
        >> task_margin_short_sale
        >> delay_after_margin
        >> task_future_daily
    )
