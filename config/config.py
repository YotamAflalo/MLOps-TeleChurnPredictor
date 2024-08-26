result_columns = ['TotalCharges','Month-to-month','One year','Two year','PhoneService','tenure']
INPUT_TYPE=["csv"]
INPUT_DIR = '.../data/batch_input/'
MODEL_PATH = '.../models/churn_model.pickle'
OUTPUT_DIR = '.../data/batch_results'
REQUIRED_COLUMNS = ['Contract', 'tenure']
OPTIONAL_COLUMNS = ['TotalCharges', 'PhoneService']
DATABASE_URL = "postgresql://user:password@db:5432/api_logs"

