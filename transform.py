import pandas as pd

def transform_df(dataset:pd.DataFrame):
    # Nulls:
    dataset['TotalCharges'] = dataset['TotalCharges'].fillna(2279) # 2279 mean value in data
    dataset['TotalCharges'] = dataset['TotalCharges'].str.replace(' ','2279') # remove space string in data
    dataset['TotalCharges'] = dataset['TotalCharges'].astype(float)

    # Contract is a importnent feature in the model and cant be null, in case of null the model will not predict and need to sand alert.
    dataset['Contract'] = dataset['Contract'].dropna()

    dataset['PhoneService'].fillna('No')

    dataset['tenure'] = dataset['tenure'].fillna(dataset['tenure'].mean())
    dataset['PhoneService'] = dataset['PhoneService'].map({'Yes':1,'No':0})

    dataset = dataset.join(pd.get_dummies(dataset['Contract']).astype(int))

    return dataset