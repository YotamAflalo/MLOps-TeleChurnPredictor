import numpy as np

from fastapi import FastAPI
import pickle
from pydantic import BaseModel
from transform import transform_df
class Prediction(BaseModel): ### to edit
    test_array: list = []
    

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
    test_array =  transform_df(pred.test_array)
    test_array = np.array(test_array)
    
    prediction_result = pickled_model.predict([test_array])
    print(prediction_result)
    return {"prediction": prediction_result.tolist()[0]}
