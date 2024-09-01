import os

result_columns = ['TotalCharges','Month-to-month','One year','Two year','PhoneService','tenure']

REQUIRED_COLUMNS = ['Contract', 'tenure']
OPTIONAL_COLUMNS = ['TotalCharges', 'PhoneService']
DATABASE_URL = "postgresql://user:password@db:5432/api_logs"
# bach processing configurations:
# can accept either "csv" or "db" or "both"
# INPUT_TYPE = ["csv", "db", "both"]
# OUTPUT_TYPE = ["csv", "db", "both"]
INPUT_TYPE = ["csv"]
OUTPUT_TYPE = ["db"]
INPUT_DIR = '.../data/batch_input/'
MODEL_PATH = '.../models/churn_model.pickle'
OUTPUT_DIR = '.../data/batch_results'
DRIVER_CLASS_NAME = 'org.postgresql.Driver'
JDBC_URL="DATABASE_URL", "postgresql://user:password@db:5432/api_logs"
USERNAME='user'
PASSWORD='password'
INPUT_TABLE='batch_input'
OUTPUT_TABLE='batch_output'