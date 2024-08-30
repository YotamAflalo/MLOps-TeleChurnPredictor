import apache_beam as beam
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io.filesystems import FileSystems
import argparse
import os
import pickle
from config.config import (
    INPUT_TYPE, OUTPUT_TYPE, INPUT_DIR, MODEL_PATH, OUTPUT_DIR,
    DRIVER_CLASS_NAME, JDBC_URL, USERNAME, PASSWORD, INPUT_TABLE, OUTPUT_TABLE
)

class ParseCsvRow(beam.DoFn):
    def process(self, element):
        # Implement CSV parsing logic here
        return [dict(zip(self.get_csv_headers(), element.split(',')))]

    @staticmethod
    def get_csv_headers():
        # Define your CSV headers here
        return ['customerID', 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'tenure',
                'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity',
                'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV',
                'StreamingMovies', 'Contract', 'PaperlessBilling', 'PaymentMethod',
                'MonthlyCharges', 'TotalCharges']

class Predict(beam.DoFn):
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None

    def setup(self):
        with open(self.model_path, 'rb') as f:
            self.model = pickle.load(f)

    def process(self, element):
        # Implement prediction logic here
        features = [element[col] for col in self.get_feature_columns()]
        prediction = self.model.predict([features])[0]
        return [{
            'customer_id': element['customerID'],
            'prediction': float(prediction)
        }]

    @staticmethod
    def get_feature_columns():
        # Define your feature columns here
        return ['tenure', 'MonthlyCharges', 'TotalCharges']

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', default=INPUT_DIR)
    parser.add_argument('--output_path', default=OUTPUT_DIR)
    known_args, pipeline_args = parser.parse_known_args()

    # This will give you more information about what files (if any) 
    # are being found by the file system abstraction layer used by Apache Beam.
    full_path = os.path.abspath(known_args.input_path + '*.csv')
    print(f"Attempting to read files from: {full_path}")
    # This will give you more information about what files (if any) 
    # are being found by the file system abstraction layer used by Apache Beam.
    pattern = known_args.input_path + '*.csv'
    match_result = FileSystems.match([pattern])[0]
    print(f"Found {len(match_result.metadata_list)} files matching the pattern")
    for metadata in match_result.metadata_list:
        print(f"  {metadata.path}")

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Read input
        if 'csv' in INPUT_TYPE:
            input_data = (
                p | 'ReadInput' >> ReadFromText(known_args.input_path + '*.csv')
                | 'ParseCSV' >> beam.ParDo(ParseCsvRow())
            )
        else:
            # Implement other input types if needed
            raise ValueError(f"Unsupported input type: {INPUT_TYPE}")

        # Predict
        predictions = input_data | 'Predict' >> beam.ParDo(Predict(MODEL_PATH))

        # Write output
        if 'db' in OUTPUT_TYPE:
            predictions | 'WriteToDB' >> WriteToJdbc(
                table_name=OUTPUT_TABLE,
                driver_class_name=DRIVER_CLASS_NAME,
                jdbc_url=JDBC_URL,
                username=USERNAME,
                password=PASSWORD,
                classpath=['org.postgresql:postgresql:42.2.16']
            )
        else:
            # Implement other output types if needed
            raise ValueError(f"Unsupported output type: {OUTPUT_TYPE}")

if __name__ == '__main__':
    run()