import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
import pickle

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
        X_['TotalCharges'] = X_['TotalCharges'].fillna(self.total_charges_fill_value)
        X_['TotalCharges'] = X_['TotalCharges'].astype(str).str.replace(' ', '')
        X_['TotalCharges'] = pd.to_numeric(X_['TotalCharges'], errors='coerce')
        X_['TotalCharges'] = X_['TotalCharges'].fillna(self.total_charges_fill_value)

        # Handle PhoneService
        phone_service_imputed = self.phone_service_imputer.transform(X_[['PhoneService']])
        X_['PhoneService'] = pd.Series(phone_service_imputed.ravel(), index=X_.index)
        X_['PhoneService'] = X_['PhoneService'].map(self.phone_service_map)
        #if 'PhoneService' not in X_.columns: X_['PhoneService']=0
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
    X = dataset = pd.read_csv(r'C:\Users\yotam\Desktop\mlops_learning\mid_project\data\raw\original_dataset.csv')


    artifact_path = 'artifacts/missing_value_handler_update.pkl'
    
    create_and_save_artifact(X, artifact_path)
    X_transformed = load_and_use_artifact(X, artifact_path)
    print(X_transformed.iloc[:20])