import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import pickle


REQUIRED_COLUMNS = ['TotalCharges', 'Contract', 'tenure','PhoneService']
OPTIONAL_COLUMNS = []

# Define a custom transform to clean and preprocess data

def discard_incomplete(data):
    """Filters out records that don't have an information about Contract type."""
    return len(data['Contract']) > 0




class CleanData(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        
        # Fill nulls and clean data
        dataset['TotalCharges'] = dataset['TotalCharges'].fillna(2279)
        dataset['TotalCharges'] = dataset['TotalCharges'].replace(' ', '2279')
        dataset['TotalCharges'] = dataset['TotalCharges'].astype(float)
        #dataset['Contract'] = dataset['Contract'].dropna()
        dataset['PhoneService'] = dataset['PhoneService'].fillna('No')
        dataset['tenure'] = dataset['tenure'].fillna(dataset['tenure'].mean())
        
        # Feature engineering
        dataset['PhoneService'] = dataset['PhoneService'].map({'Yes': 1, 'No': 0})
        dataset = dataset.join(pd.get_dummies(dataset['Contract']).astype(int))
        
        return [dataset.to_dict('records')[0]]

# Define a custom transform to make predictions
class Predict(beam.DoFn):
    def __init__(self, model):
        self.model = model
        
    def process(self, element):
        dataset = pd.DataFrame([element])
        result_columns = ['TotalCharges', 'Month-to-month', 'One year', 'Two year', 'PhoneService', 'tenure']
        prediction = self.model.predict(dataset[result_columns])
        element['prediction'] = prediction[0]
        return [element]

def run():
    # Load the pre-trained model
    with open('churn_model.pickle', 'rb') as f:
        model = pickle.load(f)

    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    (p
     | 'ReadData' >> beam.io.ReadFromText('data/database_input.csv', skip_header_lines=1)
     | 'ParseCSV' >> beam.Map(lambda line: dict(zip(['column1', 'column2', ...], line.split(','))))
     | 'CleanData' >> beam.ParDo(CleanData())
     | 'Predict' >> beam.ParDo(Predict(model))
     | 'WriteResults' >> beam.io.WriteToText('predictions_output.txt')
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
