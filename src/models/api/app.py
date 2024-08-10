import numpy as np
import pandas as pd
from fastapi import FastAPI
import pickle
from pydantic import BaseModel
#from transform import transform_df
from src.data.artifact_preparation import CustomMissingValueHandler
import os
import sys
from pathlib import Path

# Add the 'src' directory to the Python path
src_dir = Path(__file__).resolve().parents[2]
sys.path.append(str(src_dir))
# from data.artifact_preparation import CustomMissingValueHandler
# Construct the path to the artifact
artifact_path = src_dir / 'data' / 'artifacts' / 'missing_value_handler.pkl'

class Prediction(
    BaseModel):  #####צריך לטפל בשדות ולוודא שהם יתאימו לשדות של הטבלה. בנוסף צריך להוסיף אפשרות שהוא יקבל שדות אחרות שלא יכנסו פנימה (כל הזבל)
    TotalCharges: str# = Field(alias='TotalCharges')
    Contract: str# = Field(alias='Contract')
    PhoneService: str# = Field(alias='PhoneService')
    tenure: int# = Field(alias='tenure')

    class Config:
        allow_population_by_field_name = True


app = FastAPI()


@app.get("/")
def read_root():
    return {"instractions": """please provide json file in the following format: 
            {
                "TotalCharges":str,
                'Contract':str,
                'PhoneService':str,
                'tenure': int
            } """}


@app.post("/predict/")
def predict(pred: Prediction):
    # Load the model
    # file_model = open("churn_model.pkl","rb")
    # pickled_model = pickle.load(file_model)

    # Contains a single sample.
    with open('churn_model.pickle', 'rb') as f:
        rf_model = pickle.load(f)
    input_data = pd.DataFrame([pred.dict(by_alias=True)])
    handler = CustomMissingValueHandler.load(str(artifact_path))    
    input_data = handler.transform(input_data)

    # input_data = np.array(input_data)

    # prediction_result = pickled_model.predict([input_data])

    
    result_columns = ['TotalCharges', 'Month-to-month', 'One year', 'Two year', 'PhoneService', 'tenure']
    prediction_result = rf_model.predict(input_data[result_columns])
    print(prediction_result)
    return {"prediction": prediction_result.tolist()[0]}
