import numpy as np
import pandas as pd
from fastapi import FastAPI
import pickle
from pydantic import BaseModel
from transform import transform_df

    
from pydantic import BaseModel, Field

class Prediction(BaseModel):  #####צריך לטפל בשדות ולוודא שהם יתאימו לשדות של הטבלה. בנוסף צריך להוסיף אפשרות שהוא יקבל שדות אחרות שלא יכנסו פנימה (כל הזבל)
    TotalCharges: str = Field(alias='TotalCharges')
    Contract: str = Field(alias='Contract')
    PhoneService: str = Field(alias='PhoneService')
    tenure: int = Field(alias='tenure')
    
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
    #file_model = open("churn_model.pkl","rb")
    #pickled_model = pickle.load(file_model)
    
    #Contains a single sample.
    input_data = pd.DataFrame([pred.dict(by_alias=True)])
    input_data =  transform_df(input_data)
    for val in ['Month-to-month','One year','Two year']:
        if val not in input_data.index:
            input_data[val] = 0
    #input_data = np.array(input_data)
    
    #prediction_result = pickled_model.predict([input_data])

    with open('churn_model.pickle', 'rb') as f:
        rf_model = pickle.load(f)
    result_columns = ['TotalCharges','Month-to-month','One year','Two year','PhoneService','tenure']
    prediction_result = rf_model.predict(input_data[result_columns])
    print(prediction_result)
    return {"prediction": prediction_result.tolist()[0]}
