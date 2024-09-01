import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import schedule
import time
from whylogs.api.writer.whylabs import WhyLabsWriter
from whylogs.core import DatasetProfile
import pytz

# Database connection
DATABASE_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# WhyLabs configuration
WHYLABS_API_KEY = os.environ.get("WHYLABS_API_KEY",'ZZZhY9vhvy.l764MW6fPuUBIsQ5Nz22KDnb9s7JsJoRPUirpAnljqE7yGTBt5NnW')
WHYLABS_ORG_ID = os.environ.get("WHYLABS_ORG_ID")
WHYLABS_DATASET_ID = os.environ.get("WHYLABS_API_DATASET_ID")

# Set the timezone to Israel Standard Time
israel_tz = pytz.timezone('Asia/Jerusalem')

def fetch_data():
    yesterday = datetime.now(israel_tz) - timedelta(days=1)
    yesterday_noon = yesterday.replace(hour=12, minute=00, second=0, microsecond=0)
    
    query = f"""
    SELECT * FROM api_logs
    WHERE timestamp >= '{yesterday_noon}'
    """
    
    return pd.read_sql(query, engine)

def log_to_whylabs():
    print(f"Starting log_to_whylabs function at {datetime.now(israel_tz)}")
    data = fetch_data()
    print(f"Fetched {len(data)} records")
    
    if not data.empty:
        profile = DatasetProfile(dataset_timestamp=datetime.now(israel_tz))
        profile.track(data)
        
        writer = WhyLabsWriter(
            api_key=WHYLABS_API_KEY,
            org_id=WHYLABS_ORG_ID,
            dataset_id=WHYLABS_DATASET_ID
        )
        writer.write(profile)
        print(f"Successfully logged {len(data)} records to WhyLabs at {datetime.now(israel_tz)}")
    else:
        print("No new data to log")

def run_at_specific_time():
    israel_time = datetime.now(israel_tz)
    if israel_time.hour == 12 and israel_time.minute == 00:
        log_to_whylabs()

def run_daily_job():
    while True:
        run_at_specific_time()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    print(f"Starting script at {datetime.now(israel_tz)}")
    # Run the job immediately when the script starts
    log_to_whylabs()
    # Then run the daily job
    run_daily_job()