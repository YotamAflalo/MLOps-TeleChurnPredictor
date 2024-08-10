import pandas as pd
from data_preparation import basic_preparation
from feature_engineering import map_categorical, create_features

def transform_df(df):
    good_columns = [
        'customerID', 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'tenure', 'PhoneService',
        'MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
        'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract', 'PaperlessBilling',
        'PaymentMethod', 'MonthlyCharges', 'TotalCharges'
    ]
    rellevent_columns = ['customerID','tenure', 'PhoneService','TotalCharges', 'Contract']
    df = basic_preparation(df, good_columns)
    df = map_categorical(df)
    df = create_features(df)
    return df