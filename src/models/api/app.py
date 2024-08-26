import numpy as np
import pandas as pd
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session # for connecting to the database
import pickle
from pydantic import BaseModel, Extra #to ensure the integrity of the input
import os
import sys
from pathlib import Path
from typing import Union, Optional #to ensure the integrity of the input
from prometheus_fastapi_instrumentator import Instrumentator #for logging
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON # for connecting to the database
from sqlalchemy.ext.declarative import declarative_base# for connecting to the database
from sqlalchemy.orm import sessionmaker# for connecting to the database
from datetime import datetime
from sqlalchemy.exc import OperationalError# for connecting to the database
import time

from src.data.artifact_preparation import CustomMissingValueHandler
from config.config import result_columns

# Add the 'src' directory to the Python path
src_dir = Path(__file__).resolve().parents[2]
sys.path.append(str(src_dir))

artifact_path = src_dir / 'data' / 'artifacts' / 'missing_value_handler_original_data.pkl'

class CustomUnpickler(pickle.Unpickler):
    '''Used to open the transformer we built'''
    def find_class(self, module, name):
        if name == 'CustomMissingValueHandler':
            return CustomMissingValueHandler
        return super().find_class(module, name)

def load_custom_handler(filepath):
    with open(filepath, 'rb') as f:
        unpickler = CustomUnpickler(f)
        return unpickler.load()

class Prediction(BaseModel):
    TotalCharges: Optional[Union[str, int]]
    Contract: str
    PhoneService: Optional[str]
    tenure: Optional[int]

    class Config:
        allow_population_by_field_name = True
        extra = Extra.allow 

#connecting to the db
# DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@db:5432/api_logs")
# Check if we're running in a test environment

IS_TESTING = os.environ.get('TESTING') == 'True'

if not IS_TESTING:
    print('api activate - not in test mode')
    # Use SQLite for testing
    # DATABASE_URL = "sqlite:///:memory:"
# else:
    # Use PostgreSQL for production
    DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:password@db:5432/api_logs")
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
    Base = declarative_base()

    class APILog(Base):
        __tablename__ = "api_logs"

        id = Column(Integer, primary_key=True, index=True)
        timestamp = Column(DateTime, default=datetime.now)
        request_data = Column(JSON)
        prediction = Column(Integer)

    Base.metadata.create_all(bind=engine)

    def get_db():
        db = SessionLocal()
        retries = 5
        while retries > 0:
            try:
                db = SessionLocal()
                yield db
            except OperationalError:
                retries -= 1
                time.sleep(1)
            finally:
                db.close()
        if retries == 0:
            db.close()
            raise Exception("Could not connect to the database after multiple attempts")
else:
    def get_db():
        pass

app = FastAPI()
port = int(os.environ.get('PORT', 8005))

@app.get("/")
def read_root():
    return {"instructions": """Please provide a JSON file in the following format: 
            {
                "TotalCharges": str or int,
                "Contract": str,
                "PhoneService": str,
                "tenure": int
            }"""}

@app.post("/predict/")  
def predict(pred: Prediction, db: Session = Depends(get_db)):
    with open('models/churn_model.pickle', 'rb') as f:
        rf_model = pickle.load(f)
    input_data = pd.DataFrame([pred.dict(by_alias=True)])
    handler = load_custom_handler('models/missing_value_handler_original_data.pkl')
    input_data = handler.transform(input_data)
    
    prediction_result = rf_model.predict(input_data[result_columns])
    
    if not IS_TESTING:
        # Store the request and prediction in the database
        log_entry = APILog(
            request_data=pred.dict(),
            prediction=int(prediction_result[0])
        )
        db.add(log_entry)
        db.commit()

    return {"prediction": int(prediction_result[0])}

Instrumentator().instrument(app).expose(app)

####added for the tests - to delete if make problame to the compose
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)