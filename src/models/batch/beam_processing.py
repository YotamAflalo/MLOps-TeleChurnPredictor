import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import pickle
import datetime
import os
import csv
from datetime import datetime, timedelta
import whylogs as why
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.orm import sessionmaker
from config.config import DRIVER_CLASS_NAME, PASSWORD, JDBC_URL, USERNAME, PASSWORD, INPUT_TABLE, OUTPUT_TABLE, INPUT_TYPE, OUTPUT_TYPE

current_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths relative to the current script
INPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_input')
MODEL_PATH = os.path.join(current_dir, '..', '..', '..', 'models', 'churn_model.pickle')
OUTPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_results')

# Define whylogs configuration using environment variables
whylabs_org_id = os.environ.get("WHYLABS_ORG_ID")
whylabs_api_key = os.environ.get("WHYLABS_API_KEY")
whylabs_dataset_id = os.environ.get("WHYLABS_DATASET_ID")

# Set the environment variables for whylogs
os.environ["WHYLABS_DEFAULT_ORG_ID"] = whylabs_org_id
os.environ["WHYLABS_API_KEY"] = whylabs_api_key
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = whylabs_dataset_id


# Define the SQLAlchemy engine
engine = create_engine(JDBC_URL)
Session = sessionmaker(bind=engine)

def discard_incomplete(data):
    """Filters out records that don't have an information about Contract type."""
    return len(data['Contract']) > 0 and len(data['tenure']) > 0

# Define the table structure
metadata = MetaData()
output_table = Table(OUTPUT_TABLE, metadata,
    Column('id', Integer, primary_key=True),
    Column('TotalCharges', Float),
    Column('Month-to-month', Integer),
    Column('One year', Integer),
    Column('Two year', Integer),
    Column('PhoneService', Integer),
    Column('tenure', Integer),
    Column('prediction', Integer),
    Column('timestamp', DateTime, default=datetime.utcnow)
)

# Create the table if it doesn't exist
metadata.create_all(engine)

class DiscardIncompleteDoFn(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        dataset.reset_index(inplace=True)
        dataset.dropna(axis=1, subset=['Contract', 'tenure'])
        return [dataset.to_dict('records')[0]]


def validate_and_transform(value, expected_type, default_value):
    if value in [' ', '', None]:
        return default_value
    try:
        return expected_type(value)
    except (ValueError, TypeError):
        print(f"Warning: Invalid value {value}. Using default value {default_value}")
        return default_value


class TransformData(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        
        if 'TotalCharges' in dataset.columns:
            dataset['TotalCharges'] = dataset['TotalCharges'].apply(
                    lambda x: validate_and_transform(x, float, 2279.0)
            )
        else:
            dataset['TotalCharges'] = 2279.0
        
        if 'PhoneService' in dataset.columns:
            dataset['PhoneService'] = dataset['PhoneService'].fillna('No')
            dataset['PhoneService'] = dataset['PhoneService'].map(lambda x: 1 if x == 'Yes' else 0)
        else:
            dataset['PhoneService'] = 0
        
        dataset = dataset.join(pd.get_dummies(dataset['Contract']).astype(int))
        
        for val in ['Month-to-month', 'One year', 'Two year']:
            if val not in dataset.columns:
                dataset[val] = 0
        
        return [dataset.to_dict('records')[0]]


def get_csv_headers(file_path):
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        headers = next(csv_reader)
    return headers


class Predict(beam.DoFn):
    def __init__(self, model):
        self.model = model
    
    def process(self, element):
        dataset = pd.DataFrame([element])
        result_columns = ['TotalCharges', 'Month-to-month', 'One year', 'Two year', 'PhoneService', 'tenure']
        prediction = self.model.predict(dataset[result_columns])
        element['prediction'] = prediction[0]
        return [element]


def parse_csv_line(line, headers):
    values = line.split(',')
    return dict(zip(headers, values))


def read_from_db(pipeline, db_url=JDBC_URL, table_name=INPUT_TABLE, username=USERNAME, password=PASSWORD):
    last_24_hours = datetime.now() - timedelta(hours=24)
    query = f"SELECT * FROM {table_name} WHERE timestamp_column >= '{last_24_hours}'"
    return (
            pipeline
            | 'ReadFromDB' >> beam.io.ReadFromJdbc(
            table_name=table_name,
            query=query,
            driver_class_name='org.postgresql.Driver',
            jdbc_url=db_url,
            username=username,
            password=password
    )
    )



def write_to_db(data):
    def process(element):
        session = Session()
        try:
            new_record = output_table.insert().values(**element)
            session.execute(new_record)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error writing to database: {str(e)}")
        finally:
            session.close()

    return (
        data
        | 'Write to DB' >> beam.Map(process)
    )

def write_results(output_type, data):
    time = datetime.now().strftime('%d_%H_%M')
    if 'csv' in output_type:
        output_path = os.path.join(OUTPUT_DIR, f"output_{time}.csv")
        data = (data | 'WriteToCsv' >> beam.io.WriteToText(output_path))
        print(f"Results written to {output_path}")
    if 'db' in output_type:
        write_to_db(data)
        print(f"Results written to {OUTPUT_TABLE}")

def run(input_type=INPUT_TYPE, output_type=OUTPUT_TYPE, db_url=None, input_table_name=None, output_table_name=None,
        driver_class_name=DRIVER_CLASS_NAME, jdbc_url=JDBC_URL, username=USERNAME, password=PASSWORD):
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    
    options = PipelineOptions()
    
    if "csv" in input_type or "both" in input_type :
        for file in os.listdir(INPUT_DIR):
            # log csv to whylogs
            dataset = pd.read_csv(os.path.join(INPUT_DIR, file))
            results = why.log(dataset)
            results.writer("whylabs").write()
            p = beam.Pipeline(options=options)
            if file.endswith(".csv"):
                DATA_PATH = os.path.join(INPUT_DIR, file)
                print(f"Processing file: {DATA_PATH}")
                headers = get_csv_headers(DATA_PATH)
                data = (
                        p
                        | 'ReadData' >> beam.io.ReadFromText(DATA_PATH, skip_header_lines=1)
                        | 'ParseCSV' >> beam.Map(lambda line: parse_csv_line(line, headers))
                        | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
                        | 'TransformData' >> beam.ParDo(TransformData())
                        | 'Predict' >> beam.ParDo(Predict(model))
                )
                # write results
                write_results(output_type,data)
                # run the pipeline
                p.run().wait_until_finish()
                #upload results to WhyLabs
                results = why.log(data)
                results.writer("whylabs").write()
                os.remove(DATA_PATH)
                print(f"File {DATA_PATH} deleted.")
    
    if "db" in input_type or "both" in input_type  :
        # log db to whylogs
        data = pd.read_sql_query(f"SELECT * FROM {INPUT_TABLE}", engine)
        data.to_whylogs().log()
        p = beam.Pipeline(options=options)
        print("Reading from database")
        data = read_from_db(p)
        data = (
                data
                | 'DeleteIncompleteData' >> beam.ParDo(DiscardIncompleteDoFn())
                | 'TransformData' >> beam.ParDo(TransformData())
                | 'Predict' >> beam.ParDo(Predict(model))
        )
        # write results
        write_results(output_type, data)
        # upload results to WhyLabs
        results = why.log(data)
        results.writer("whylabs").write()
        # run the pipeline
        p.run().wait_until_finish()


if __name__ == '__main__':
    run()