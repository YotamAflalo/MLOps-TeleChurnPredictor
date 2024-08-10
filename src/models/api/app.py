import numpy as np
import pandas as pd
from fastapi import FastAPI
import pickle
from pydantic import BaseModel, Extra
#from transform import transform_df
from src.data.artifact_preparation import CustomMissingValueHandler
import os
import sys
from pathlib import Path
from typing import Union, Optional

# Add the 'src' directory to the Python path
src_dir = Path(__file__).resolve().parents[2]
sys.path.append(str(src_dir))
# from data.artifact_preparation import CustomMissingValueHandler
# Construct the path to the artifact
artifact_path = src_dir / 'data' / 'artifacts' / 'missing_value_handler.pkl'
class CustomUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if name == 'CustomMissingValueHandler':
            return CustomMissingValueHandler
        return super().find_class(module, name)

def load_custom_handler(filepath):
    with open(filepath, 'rb') as f:
        unpickler = CustomUnpickler(f)
        return unpickler.load()
class Prediction(
    BaseModel):  
    TotalCharges: Optional[Union[str, int]]# = Field(alias='TotalCharges')
    Contract: str# = Field(alias='Contract')
    PhoneService: Optional[str]# = Field(alias='PhoneService')
    tenure: Optional[int]# = Field(alias='tenure')

    class Config:
        allow_population_by_field_name = True
        extra = Extra.allow 


app = FastAPI()

@app.get("/")
def read_root():
    return {"instractions": """please provide json file in the following format: 
            {
                "TotalCharges":str or int,
                'Contract':str,
                'PhoneService':str,
                'tenure': int
            } """
            }


@app.post("/predict/")
def predict(pred: Prediction):
    # Load the model
    # file_model = open("churn_model.pkl","rb")
    # pickled_model = pickle.load(file_model)

    # Contains a single sample.
    with open('models/churn_model.pickle', 'rb') as f:
        rf_model = pickle.load(f)
    input_data = pd.DataFrame([pred.dict(by_alias=True)])
    handler = load_custom_handler('models/missing_value_handler.pkl')
    #handler = CustomMissingValueHandler.load('models/missing_value_handler_update.pkl')#(str(artifact_path))    
    input_data = handler.transform(input_data)

    # input_data = np.array(input_data)

    # prediction_result = pickled_model.predict([input_data])

    
    result_columns = ['TotalCharges', 'Month-to-month', 'One year', 'Two year', 'PhoneService', 'tenure']
    prediction_result = rf_model.predict(input_data[result_columns])
    print(prediction_result)
    return {"prediction": prediction_result.tolist()[0]}
