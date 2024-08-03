import numpy as np

from fastapi import FastAPI
import pickle
from pydantic import BaseModel
from transform import transform_df

    
from pydantic import BaseModel, Field

class Prediction(BaseModel):
    TotalCharges: float = Field(alias='TotalCharges')
    Month_to_month: int = Field(alias='Month-to-month')
    One_year: int = Field(alias='One year')
    Two_year: int = Field(alias='Two year')
    PhoneService: int = Field(alias='PhoneService')
    tenure: int = Field(alias='tenure')
    
    class Config:
        allow_population_by_field_name = True
app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World3"}


@app.post("/predict/")
def predict(pred: Prediction):

    # Load the model
    file_model = open("pickle_model.pkl","rb")
    pickled_model = pickle.load(file_model)
    
    #Contains a single sample.
    input_data = pd.DataFrame([pred.dict(by_alias=True)])
    input_data =  transform_df(input_data)
    input_data = np.array(input_data)
    
    prediction_result = pickled_model.predict([input_data])
    print(prediction_result)
    return {"prediction": prediction_result.tolist()[0]}
