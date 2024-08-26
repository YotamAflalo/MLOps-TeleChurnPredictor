import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import pandas as pd
import pickle
import datetime
import os
import csv
# from config.config import INPUT_DIR,MODEL_PATH,OUTPUT_DIR

current_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths relative to the current script
INPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_input')
MODEL_PATH = os.path.join(current_dir, '..', '..', '..', 'models', 'churn_model.pickle')
OUTPUT_DIR = os.path.join(current_dir, '..', '..', '..', 'data', 'batch_results')


# Define a custom transform to clean and preprocess data

def discard_incomplete(data):
    """Filters out records that don't have an information about Contract type."""
    return len(data['Contract']) > 0 and len(data['tenure']) > 0


class discard_incomplete_dofn(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        dataset.reset_index(inplace=True)
        #print(dataset.columns)
        dataset.dropna(axis=1, subset=['Contract', 'tenure'])
        return [dataset.to_dict('records')[0]]

# validate and transform to ensure the data is in the correct format
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
        
        # Fill nulls and clean data
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
    for file in os.listdir(INPUT_DIR):
        if file.endswith(".csv"):
            DATA_PATH = os.path.join(INPUT_DIR, file)
            print(f"Processing file: {DATA_PATH}")
            # log time of prediction
            time = datetime.datetime.now().strftime('%d_%H_%M')
            # log file name
            # here will be the y-log log of the files name and time
            options = PipelineOptions()
            # define the pipeline options
            p = beam.Pipeline(options=options)
            # define the pipeline headers from the csv file
            headers = get_csv_headers(DATA_PATH)
            data = (
                    p
                    | 'ReadData' >> beam.io.ReadFromText(DATA_PATH, skip_header_lines=1)
                    | 'SplitData' >> beam.Map(lambda x: x.split(','))
                    | 'FormatToDict' >> beam.Map(lambda x: dict(zip(headers, x)))
                    | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
                    | 'TransformData' >> beam.ParDo(TransformData())
                    | 'Predict' >> beam.ParDo(Predict(model))
                    | 'WriteResults' >> beam.io.WriteToText(os.path.join(OUTPUT_DIR, f'results_{file}_{time}.csv'))
            )
            
            
            p.run().wait_until_finish()
            print(f"Processing of {DATA_PATH} finished.")
            # delete the file after from batch_input
            os.remove(DATA_PATH)
            print(f"File {DATA_PATH} deleted.")
            
            
if __name__ == '__main__':
    run()
