import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import pandas as pd
import pickle
import datetime
# import sys
# from pathlib import Path
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
import csv

# Define paths relative to the current script
DATA_PATH = os.path.join(current_dir, '..', '..', '..', 'data', 'raw', 'database_input.csv')
MODEL_PATH = os.path.join(current_dir, '..', '..', '..', 'models', 'churn_model.pickle')
OUTPUT_DIR = os.path.join(current_dir, 'output')

# Define a custom transform to clean and preprocess data

def discard_incomplete(data):
    """Filters out records that don't have an information about Contract type."""
    return len(data['Contract']) > 0 and len(data['tenure']) > 0

class discard_incomplete_dofn(beam.DoFn):
        def process(self, element):
            dataset = pd.DataFrame([element])
            dataset.reset_index(inplace=True)
            #print(dataset.columns)
            dataset.dropna(axis=1,subset=['Contract','tenure'])
            return [dataset.to_dict('records')[0]]     

class TransformData(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        
        # Fill nulls and clean data
        if 'TotalCharges' in dataset.columns:
            dataset['TotalCharges'] = dataset['TotalCharges'].fillna(2279)
            dataset['TotalCharges'] = dataset['TotalCharges'].replace(' ', '2279')
            dataset['TotalCharges'] = dataset['TotalCharges'].astype(float)
        else:
            dataset['TotalCharges'] = 2279.0

        if 'PhoneService' in dataset.columns:
            dataset['PhoneService'] = dataset['PhoneService'].fillna('No')
            dataset['PhoneService'] = dataset['PhoneService'].map({'Yes': 1, 'No': 0})
        else:
            dataset['PhoneService'] = 0

        # One-hot encoding for Contract column
        dataset = dataset.join(pd.get_dummies(dataset['Contract']).astype(int))

        # Ensure all one-hot encoded columns are present
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
def print_debug_info(element):
    print(f"Debug Info: {element}")
    return [element]
def run():
    # Load the pre-trained model
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    time =  str(datetime.datetime.now()).replace(' ','_').replace(':','_')
    options = PipelineOptions()

    p = beam.Pipeline(options=options)
    headers = get_csv_headers(DATA_PATH)
    data = (
        p 
        | 'ReadData' >> beam.io.ReadFromText(DATA_PATH, skip_header_lines=1)
        | 'SplitData' >> beam.Map(lambda x: x.split(','))
        | 'FormatToDict' >> beam.Map(lambda x: dict(zip(headers, x)))
        # | 'FormatToDict' >> beam.Map(lambda x: {"customerID": x[1], "tenure": x[6],
        #                                          "PhoneService": x[7], "Contract": x[16], "TotalCharges": x[20]})
        | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
        | 'TransformData' >> beam.ParDo(TransformData())
        | 'Predict' >> beam.ParDo(Predict(model))
        | 'WriteResults' >> beam.io.WriteToText(os.path.join(OUTPUT_DIR, f'results_{time}.csv'))
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
