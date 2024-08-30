import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import pandas as pd
import pickle
import datetime
import os
import csv
from apache_beam.io.jdbc import ReadFromJdbc, WriteToJdbc

# Import configuration
from config.config import (
    INPUT_TYPE, OUTPUT_TYPE, INPUT_DIR, MODEL_PATH, OUTPUT_DIR,
    DRIVER_CLASS_NAME, JDBC_URL, USERNAME, PASSWORD,
    INPUT_TABLE, OUTPUT_TABLE, REQUIRED_COLUMNS, OPTIONAL_COLUMNS
)

current_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths relative to the current script
INPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_input')
MODEL_PATH = os.path.join(current_dir, '..', '..', '..', 'models', 'churn_model.pickle')
OUTPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_results')



# Load the pre-trained model
with open(MODEL_PATH, 'rb') as f:
    model = pickle.load(f)

class DisCardIncompleteDofn(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        dataset.reset_index(inplace=True)
        dataset.dropna(axis=1, subset=REQUIRED_COLUMNS)
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
        
        for column in OPTIONAL_COLUMNS:
            if column == 'TotalCharges':
                dataset[column] = dataset[column].apply(
                    lambda x: validate_and_transform(x, float, 2279.0)
                )
            elif column == 'PhoneService':
                dataset[column] = dataset[column].fillna('No')
                dataset[column] = dataset[column].map(lambda x: 1 if x == 'Yes' else 0)
        
        dataset = dataset.join(pd.get_dummies(dataset['Contract']).astype(int))
        
        for val in ['Month-to-month', 'One year', 'Two year']:
            if val not in dataset.columns:
                dataset[val] = 0
        
        return [dataset.to_dict('records')[0]]

class Predict(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        result_columns = ['TotalCharges', 'Month-to-month', 'One year', 'Two year', 'PhoneService', 'tenure']
        prediction = model.predict(dataset[result_columns])
        element['prediction'] = prediction[0]
        return [element]

def get_csv_headers(file_path):
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        headers = next(csv_reader)
    return headers

def run():
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Input
    if "csv" in INPUT_TYPE or "both" in INPUT_TYPE:
        csv_files = [os.path.join(INPUT_DIR, f) for f in os.listdir(INPUT_DIR) if f.endswith(".csv")]
        headers = get_csv_headers(csv_files[0])
        csv_data = (
            p
            | "ReadCSV" >> beam.Create(csv_files)
            | "ReadCSVFiles" >> beam.FlatMap(lambda file: beam.io.ReadFromText(file, skip_header_lines=1))
            | "ParseCSV" >> beam.Map(lambda line: dict(zip(headers, line.split(','))))
        )

    if "db" in INPUT_TYPE or "both" in INPUT_TYPE:
        db_data = (
            p
            | "ReadFromJDBC" >> ReadFromJdbc(
                table_name=INPUT_TABLE,
                driver_class_name=DRIVER_CLASS_NAME,
                jdbc_url=JDBC_URL,
                username=USERNAME,
                password=PASSWORD
            )
        )

    # Combine data sources if both are used
    if "both" in INPUT_TYPE:
        data = (
            (csv_data, db_data)
            | "MergeInputs" >> beam.Flatten()
        )
    elif "csv" in INPUT_TYPE:
        data = csv_data
    else:
        data = db_data

    # Processing
    processed_data = (
        data
        | "DeleteIncompleteData" >> beam.ParDo(DisCardIncompleteDofn())
        | "TransformData" >> beam.ParDo(TransformData())
        | "Predict" >> beam.ParDo(Predict())
    )

    # Output
    if "csv" in OUTPUT_TYPE or "both" in OUTPUT_TYPE:
        time = datetime.datetime.now().strftime('%d_%H_%M')
        processed_data | "WriteToCSV" >> beam.io.WriteToText(
            os.path.join(OUTPUT_DIR, f'results_{time}.csv')
        )

    if "db" in OUTPUT_TYPE or "both" in OUTPUT_TYPE:
        processed_data | "WriteToDB" >> WriteToJdbc(
            table_name=OUTPUT_TABLE,
            driver_class_name=DRIVER_CLASS_NAME,
            jdbc_url=JDBC_URL,
            username=USERNAME,
            password=PASSWORD,
            expansion_service='localhost:8097'
        )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()