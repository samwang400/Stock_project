import datetime
from typing import List

from stockdata.crawler import (
    taiwan_stock_price,
    taiwan_futures_daily,
    taiwan_stock_info,
    taiwan_share_holding,
    taiwan_institutional_investor,
    taiwan_margin_short_sale
)
from stockdata.backend.db import get_db_router
from stockdata.backend.db.db import update2mysql_by_sql, update2mysql_by_sql_for_info


def is_weekend(day: int) -> bool:
    """Return True if the day index is Saturday or Sunday"""
    return day in (5, 6)


def gen_date_list(start_date: str, end_date: str) -> List[str]:
    """Generate a list of dates excluding weekends"""
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    delta_days = (end - start).days + 1

    dates = [start + datetime.timedelta(days=i) for i in range(delta_days)]
    return [d.strftime("%Y-%m-%d") for d in dates if not is_weekend(d.weekday())]


# Mapping tasks that do not require date range
PIPELINES_NO_DATE = {
    "taiwan_stock_info": lambda router: update_stock_info(router),
    "taiwan_share_holding": lambda router: update_share_holding(router),
}

# Mapping tasks that require date range
PIPELINES_WITH_DATE = {
    "taiwan_stock_price": lambda router, date: update_stock_price(router, date),
    "taiwan_institutional_investor": lambda router, date: update_institutional_investor(router, date),
    "taiwan_margin_short_sale": lambda router, date: update_margin_short_sale(router, date),
    "taiwan_future_daily": lambda router, date: update_future_daily(router, date),
}

# -------------------------------------
# Pipeline functions
# -------------------------------------

def update_stock_info(router):
    df_twse, df_tpex = taiwan_stock_info.stock_info_pipeline()
    if not df_twse.empty:
        update2mysql_by_sql_for_info(df_twse, "taiwan_stock_info", router.mysql_stockdata_conn)
    if not df_tpex.empty:
        update2mysql_by_sql_for_info(df_tpex, "taiwan_stock_info", router.mysql_stockdata_conn)


def update_share_holding(router):
    df = taiwan_share_holding.share_holding_pipeline()
    if not df.empty:
        update2mysql_by_sql(df, "taiwan_share_holding", router.mysql_stockdata_conn)


def update_stock_price(router, date):
    df_twse, df_tpex = taiwan_stock_price.stock_price_pipeline(date)
    if not df_twse.empty:
        update2mysql_by_sql(df_twse, "taiwan_stock_price", router.mysql_stockdata_conn)
    if not df_tpex.empty:
        update2mysql_by_sql(df_tpex, "taiwan_stock_price", router.mysql_stockdata_conn)


def update_institutional_investor(router, date):
    df_twse, df_tpex = taiwan_institutional_investor.institutional_investor_pipeline(date)
    if not df_twse.empty:
        update2mysql_by_sql(df_twse, "taiwan_institutional_investor", router.mysql_stockdata_conn)
    if not df_tpex.empty:
        update2mysql_by_sql(df_tpex, "taiwan_institutional_investor", router.mysql_stockdata_conn)


def update_margin_short_sale(router, date):
    df_twse, df_tpex = taiwan_margin_short_sale.margin_short_sale_pipeline(date)
    if not df_twse.empty:
        update2mysql_by_sql(df_twse, "taiwan_margin_short_sale", router.mysql_stockdata_conn)
    if not df_tpex.empty:
        update2mysql_by_sql(df_tpex, "taiwan_margin_short_sale", router.mysql_stockdata_conn)


def update_future_daily(router, date):
    df = taiwan_futures_daily.future_pipeline(date)
    if not df.empty:
        update2mysql_by_sql(df, "taiwan_future_daily", router.mysql_stockdata_conn)


# -------------------------------------
# Function for Airflow to call
# -------------------------------------
def run_task(task_name: str, start_date: str = None, end_date: str = None):
    """Run a specific crawler task"""
    router = get_db_router()

    if task_name in PIPELINES_NO_DATE:
        PIPELINES_NO_DATE[task_name](router)
        return

    if task_name in PIPELINES_WITH_DATE:
        if start_date is None or end_date is None:
            raise ValueError(f"Task {task_name} requires start_date and end_date")
        for date in gen_date_list(start_date, end_date):
            PIPELINES_WITH_DATE[task_name](router, date)
        return

    raise ValueError(f"Unknown task: {task_name}")


# -------------------------------------
# CLI support (for local testing)
# -------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python main.py <task_name> [start_date end_date]")
        sys.exit(1)

    task_name = sys.argv[1]
    start_date = sys.argv[2] if len(sys.argv) > 2 else None
    end_date = sys.argv[3] if len(sys.argv) > 3 else None

    run_task(task_name, start_date, end_date)
