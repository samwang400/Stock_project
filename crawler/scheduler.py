from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import subprocess
import pytz
from datetime import datetime, timedelta


# Set time zone
tz = pytz.timezone("Asia/Taipei")
scheduler = BlockingScheduler(timezone=tz)

@scheduler.scheduled_job(CronTrigger(hour=15, minute=5))

def scheduled_task():
    today = datetime.now(tz).strftime("%Y-%m-%d")
    
    print(f"[{datetime.now(tz)}] Running ETL for {today}")

    # Run taiwan_stock_price
    subprocess.call([
        "poetry", "run", "python", "-m", "stockdata.main", "taiwan_stock_price", today, today
    ])

    # Run taiwan_future_daily
    subprocess.call([
        "poetry", "run", "python", "-m", "stockdata.main", "taiwan_future_daily", today, today
    ])
    
    # Run taiwan_future_daily
    subprocess.call([
        "poetry", "run", "python", "-m", "stockdata.main", "taiwan_stock_info"
    ])
    
    # Run taiwan_future_daily
    subprocess.call([
        "poetry", "run", "python", "-m", "stockdata.main", "taiwan_share_hodling"
    ])

if __name__ == "__main__":
    print("Scheduler started. Waiting for 15:05 Asia/Taipei every day...")
    scheduler.start()

