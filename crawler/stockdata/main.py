import sys
import typing
import time
import datetime
from stockdata.crawler.taiwan_stock_price import stock_pipeline
from stockdata.crawler.taiwan_futures_daily import future_pipeline
from stockdata.backend.db import get_db_router
from stockdata.backend.db.db import update2mysql_by_sql

def is_weekend(day: int) -> bool:
    """
    Identify whether the given day index is a weekend
    """

    return day in [5, 6]


def gen_date_list(start_date: str, end_date: str) -> typing.List[str]:
    """
    Generate date list
    """
    
    # Create start_date and end_date
    start_date = (
        datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    )

    end_date = (
        datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    )

    days = (end_date - start_date).days + 1

    # Make date_list
    date_list = [
        start_date+ datetime.timedelta(days=day) for day in range(days)
    ]
    
    # Delete weekend date
    date_list = [
        d.strftime("%Y-%m-%d") for d in date_list if not is_weekend(d.weekday())
    ]


    return date_list

# Run main
def main():

    if len(sys.argv) != 4:
        print("Usage: python main.py [taiwan_stock_price|taiwan_future_daily] start_date end_date")
        sys.exit(1)

    task = sys.argv[1]
    start_date = sys.argv[2]
    end_date = sys.argv[3]

    date_list = gen_date_list(start_date, end_date)
    router = get_db_router()
    
    if task == "taiwan_stock_price":
        
        for date in date_list:

            df_twse, df_tpex = stock_pipeline(date)  

            if not df_twse.empty:
                update2mysql_by_sql(df_twse, "taiwan_stock_price", router.mysql_stockdata_conn)

            if not df_tpex.empty:
                update2mysql_by_sql(df_tpex, "taiwan_stock_price", router.mysql_stockdata_conn)

    elif task == "taiwan_future_daily":
        
        for date in date_list:

            df = future_pipeline(date)
            
            if not df.empty:
                update2mysql_by_sql(df, "taiwan_future_daily", router.mysql_stockdata_conn)

    else:
        print(f"Unknown task: {task}")
        sys.exit(1)

if __name__ == "__main__":
    main()


