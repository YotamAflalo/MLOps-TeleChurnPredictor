import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
import pickle

import os
current_dir = os.path.dirname(os.path.abspath(__file__))

# Define paths relative to the current script
DATA_PATH = os.path.join(current_dir, '..',  '..', 'data', 'raw', 'original_dataset.csv')
OUTPUT_DIR = os.path.join(current_dir, '..',  '..', 'models', 'missing_value_handler_original_data.pkl')

def validate_and_transform(value, expected_type, default_value):
    if value in [' ', '', None]:
        return default_value
    try:
        return expected_type(value)
    except (ValueError, TypeError):
        print(f"Warning: Invalid value {value}. Using default value {default_value}")
        return default_value
class CustomMissingValueHandler(BaseEstimator, TransformerMixin):
    def __init__(self, total_charges_fill_value=2279):
        self.total_charges_fill_value = total_charges_fill_value
        self.phone_service_imputer = SimpleImputer(strategy='constant', fill_value='No')
        self.tenure_imputer = SimpleImputer(strategy='mean')
        self.phone_service_map = {'Yes': 1, 'No': 0}
        self.contract_encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')

    def fit(self, X, y=None):
        self.phone_service_imputer.fit(X[['PhoneService']])
        self.tenure_imputer.fit(X[['tenure']])
        self.contract_encoder.fit(X[['Contract']])
        self.contract_categories = self.contract_encoder.categories_[0]
        return self

    def transform(self, X):
        X_ = X.copy()
        # making sure all the relevant columns are in the df, with value or null
        expected_columns = ['TotalCharges', 'Contract', 'PhoneService', 'tenure']
        for col in expected_columns:
            if col not in X_.columns:
                X_[col] = np.nan
                if col == 'PhoneService': X_[col] = 'No'
        # Drop rows where Contract is null
        X_ = X_.dropna(subset=['Contract'])

        # Handle TotalCharges
        if 'TotalCharges' in X_.columns:
            X_['TotalCharges'] = X_['TotalCharges'].apply(
                    lambda x: validate_and_transform(x, float, 2279.0)
            )
        else:
            X_['TotalCharges'] = 2279.0

        # Handle PhoneService
        phone_service_imputed = self.phone_service_imputer.transform(X_[['PhoneService']])
        X_['PhoneService'] = pd.Series(phone_service_imputed.ravel(), index=X_.index)
        X_['PhoneService'] = X_['PhoneService'].map(lambda x: 1 if x == 'Yes' else 0)
        if 'PhoneService' not in X_.columns: X_['PhoneService']=0
        # Handle tenure
        tenure_imputed = self.tenure_imputer.transform(X_[['tenure']])
        X_['tenure'] = pd.Series(tenure_imputed.ravel(), index=X_.index)

        # One-hot encode Contract
        contract_encoded = self.contract_encoder.transform(X_[['Contract']])
        contract_df = pd.DataFrame(contract_encoded, columns=self.contract_categories, index=X_.index)

        # Join the encoded Contract columns to the original DataFrame, and make sure all of them are there
        X_ = X_.join(contract_df)
        for category in self.contract_categories:
            if category not in X_.columns:
                X_[category] = 0
        return X_

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)

    def save(self, filepath):
        with open(filepath, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)

def create_and_save_artifact(X, filepath):
    handler = CustomMissingValueHandler()
    handler.fit(X)
    handler.save(filepath)
    print(f"Artifact saved to {filepath}")
def load_and_use_artifact(X, filepath):
    handler = CustomMissingValueHandler.load(filepath)
    return handler.transform(X)

# Example usage:
if __name__ == "__main__":
    #we fit the transformer on the original train data so we consistent
    X = dataset = pd.read_csv(DATA_PATH)
    
    create_and_save_artifact(X, OUTPUT_DIR)
    X_transformed = load_and_use_artifact(X, OUTPUT_DIR)
    print(X_transformed.iloc[:20])