import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import pickle
import datetime

REQUIRED_COLUMNS = ['Contract', 'tenure']
OPTIONAL_COLUMNS = ['TotalCharges', 'PhoneService']
PATH = r"C:\Users\yotam\Desktop\mlops_learning\mid_project\data\database_input.csv" 
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
        if 'Contract' in dataset.columns:
            dataset = dataset.join(pd.get_dummies(dataset['Contract']).astype(int))

        # Ensure all one-hot encoded columns are present
        for val in ['Month-to-month', 'One year', 'Two year']:
            if val not in dataset.columns:
                dataset[val] = 0
                
        return [dataset.to_dict('records')[0]]
class headers_taker(beam.DoFn):
    def process(self, element):
        dataset = pd.DataFrame([element])
        headers = []
        for col in dataset.columns: 
            #print(col)
            headers.append(col)
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
    with open(r'C:\Users\yotam\Desktop\mlops_learning\mid_project\churn_model.pickle', 'rb') as f:
        model = pickle.load(f)
    time =  str(datetime.datetime.now()).replace(' ','_').replace(':','_')
    options = PipelineOptions()
    p = beam.Pipeline(options=options)
    # headers = (
    #     p 
    #     | 'ReadHeader' >> beam.io.ReadFromText(PATH, skip_header_lines=0)   
    #     | 'ExtractHeader' >> beam.Map(lambda line: line.split(','))
    #     #| 'justTakeTheader' >> beam.ParDo(headers_taker()) 
    #     # | 'ExtractFirstRow' >> beam.Map(lambda headers: headers[0])
    #     #| 'PrintDebug2' >> beam.ParDo(print_debug_info)
    # )
    data = (
        p 
        | 'ReadData' >> beam.io.ReadFromText(PATH, skip_header_lines=1)
        | 'SplitData' >> beam.Map(lambda x: x.split(','))
        | 'FormatToDict' >> beam.Map(lambda x: {"customerID": x[1], "tenure": x[6],
                                                 "PhoneService": x[7], "Contract": x[16], "TotalCharges": x[20]})

        #| 'ConvertToDict' >> beam.Map(lambda line, headers: parse_csv_line(line, headers), beam.pvalue.AsSingleton(headers))
        #| 'AddIndex' >> beam.ParDo(lambda i_x: AddID().process(i_x[1], i_x[0]), beam.ParDo(lambda element: enumerate(element)))
        | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
        #| 'DeleteIncompleteData' >>  beam.ParDo(discard_incomplete_dofn())
        #| 'PrintDebug2' >> beam.ParDo(print_debug_info)
        | 'TransformData' >> beam.ParDo(TransformData())
        | 'Predict' >> beam.ParDo(Predict(model))
        | 'WriteResults' >> beam.io.WriteToText(f'output_{time}.csv')
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
