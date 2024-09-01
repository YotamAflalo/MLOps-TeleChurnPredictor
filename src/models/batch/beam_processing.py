import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import numpy as np
import pickle
import datetime
import time
import shutil
import os
import csv
from datetime import datetime, timedelta
import whylogs as why
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import sys

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.append(project_root)

from config.config import DRIVER_CLASS_NAME, PASSWORD, JDBC_URL, USERNAME, INPUT_TABLE, OUTPUT_TABLE, INPUT_TYPE, OUTPUT_TYPE, JDBC_URL
IS_TESTING = os.environ.get('TESTING',"False") == 'True'

current_dir = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_input')
MODEL_PATH = os.path.join(current_dir, '..', '..', '..', 'models', 'churn_model.pickle')
OUTPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_results')
if IS_TESTING:
    INPUT_DIR = 'csv'
    OUTPUT_DIR = 'csv'
whylabs_org_id = os.environ.get("WHYLABS_ORG_ID")
whylabs_api_key = os.environ.get("WHYLABS_API_KEY")
whylabs_dataset_id = os.environ.get("WHYLABS_DATASET_ID")

engine = create_engine(JDBC_URL)
Session = sessionmaker(bind=engine)

def discard_incomplete(data):
    return len(data['Contract']) > 0 and len(data['tenure']) > 0

metadata = MetaData()
output_table = Table(OUTPUT_TABLE, metadata,
    Column('customerID', String, primary_key=True, nullable=False),
    Column('TotalCharges', Float),
    Column('MonthlyCharges', Float),
    Column('tenure', Integer),
    Column('Contract', String),
    Column('PhoneService', Integer),
    Column('prediction', Float),
    Column('timestamp', DateTime, default=datetime.utcnow)
)

metadata.drop_all(engine, tables=[output_table])
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
        
        if 'customerID' not in dataset.columns or pd.isna(dataset['customerID'].iloc[0]) or dataset['customerID'].iloc[0] == '':
            return []  # Skip this record
        
        numeric_columns = ['TotalCharges', 'MonthlyCharges', 'tenure']
        for col in numeric_columns:
            if col in dataset.columns:
                dataset[col] = dataset[col].apply(
                    lambda x: validate_and_transform(x, float, 0.0)
                )
            else:
                dataset[col] = 0.0
        
        if 'PhoneService' in dataset.columns:
            dataset['PhoneService'] = dataset['PhoneService'].fillna('No')
            dataset['PhoneService'] = dataset['PhoneService'].map(lambda x: 1 if x == 'Yes' else 0)
        else:
            dataset['PhoneService'] = 0
        
        if 'Contract' in dataset.columns:
            dataset['Contract'] = dataset['Contract'].fillna('Unknown')
        else:
            dataset['Contract'] = 'Unknown'
        
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
        if 'customerID' not in element or not element['customerID']:
            return []
        
        dataset = pd.DataFrame([element])
        contract_dummies = pd.get_dummies(dataset['Contract'])
        dataset = pd.concat([dataset, contract_dummies], axis=1)

        
        for contract_type in ['Month-to-month', 'One year', 'Two year']:
            if contract_type not in dataset.columns:
                dataset[contract_type] = 0
        
        result_columns = ['TotalCharges', 'Month-to-month', 'One year', 'Two year', 'PhoneService', 'tenure']
        prediction = self.model.predict(dataset[result_columns])
        element['prediction'] = float(prediction[0])
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
            for key, value in element.items():
                if isinstance(value, np.integer):
                    element[key] = int(value)
                elif isinstance(value, np.floating):
                    element[key] = float(value)
                elif isinstance(value, np.ndarray):
                    element[key] = value.tolist()
            
            if 'customerID' not in element:
                raise ValueError("customerID is missing from the data")
            element['customerID'] = str(element['customerID'])
            
            filtered_element = {k: v for k, v in element.items() if k in [c.name for c in output_table.columns]}
            
            for column in output_table.columns:
                if column.name not in filtered_element and not column.nullable:
                    filtered_element[column.name] = None
            
            new_record = output_table.insert().values(**filtered_element)
            session.execute(new_record)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            print(f"Skipping duplicate record for customerID: {element.get('customerID', 'Unknown')}")
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

def select_fields(element):
    return {
        'customerID': element['customerID'],
        'prediction': element['prediction'],
        'timestamp': element['timestamp']
    }
def apply_whylogs(collected_data):
                    # Use the simplified whylogs v1 API to log the data
                    results = why.log(collected_data)
                    # Write the results to WhyLabs
                    results.writer("whylabs").write()
                    return results
def run(input_type=INPUT_TYPE, output_type=OUTPUT_TYPE, db_url=None, input_table_name=None, output_table_name=None,
        driver_class_name=DRIVER_CLASS_NAME, jdbc_url=JDBC_URL, username=USERNAME, password=PASSWORD):
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    
    options = PipelineOptions()
    
    if "csv" in input_type or "both" in input_type:
        for file in os.listdir(INPUT_DIR):
            dataset = pd.read_csv(os.path.join(INPUT_DIR, file))
            if not IS_TESTING:
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
                
                write_results(output_type, data)
                p.run().wait_until_finish()
                
                selected_data = data | beam.Map(select_fields)
                collected_data = selected_data | beam.combiners.ToList()

                
                
                #return collected_data | beam.Map(apply_whylogs)
                if not IS_TESTING:
                    try:
                        results = collected_data | beam.Map(apply_whylogs)
                        results.writer("whylabs").write()
                    except:
                        print("eror in writing the results to whylabs")
                os.remove(DATA_PATH)
                print(f"File {DATA_PATH} deleted.")
    
    if "db" in input_type or "both" in input_type:
        p = beam.Pipeline(options=options)
        print("Reading from database")
        data = read_from_db(p)
        data = (
            data
            | 'DeleteIncompleteData' >> beam.ParDo(DiscardIncompleteDoFn())
            | 'TransformData' >> beam.ParDo(TransformData())
            | 'Predict' >> beam.ParDo(Predict(model))
        )
        
        write_results(output_type, data)
        if not IS_TESTING:
            try:
                # Log the data to WhyLabs
                results = why.log(data[['customerID','TotalCharges', 'Month-to-month', 'One year', 'Two year', 'PhoneService', 'tenure', 'prediction', 'timestamp']])
                results.writer("whylabs").write()
            except:
                print("eror in writing the results to whylabs")
        # Run the pipeline

        p.run().wait_until_finish()

        print("Batch processing completed.")
    

if __name__ == '__main__':
    run()