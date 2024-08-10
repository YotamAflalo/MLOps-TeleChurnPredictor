import pandas as pd
from sklearn.impute import SimpleImputer


def load_data(file_path):
    return pd.read_csv(file_path)


def drop_unnecessary_columns(df, good_columns):
    return df[good_columns]


def handle_missing_values(df):
    # Impute missing values for TotalCharges with the mean
    
    # Drop rows where Contract is null
    df = df.dropna(subset=['Contract']) 

    df['TotalCharges'] = df['TotalCharges'].str.replace(' ', '2279') # Need to remove this line and think of a better way to handle this
    df['TotalCharges'] = df['TotalCharges'].fillna(2279)
    df['TotalCharges'] = df['TotalCharges'].astype(float)
    # we can't use SimpleImputer here, in the api we getting just one row, in the batch we will get diffrent mean every time
    # we shold use the 2279 constent, or make a artifact for SimpleImputer with the original data
    # total_charges_imputer = SimpleImputer(strategy='mean')
    # df['TotalCharges'] = total_charges_imputer.fit_transform(df[['TotalCharges']])

    

    # Fill missing values for PhoneService with 'No'
    phone_service_imputer = SimpleImputer(strategy='constant', fill_value='No')
    df['PhoneService'] = phone_service_imputer.fit_transform(df[['PhoneService']])

    # Fill missing values for tenure with the mean 
    # here we also hava the same problame - we can't use the arbitrery mean in the batch as a mean..
    tenure_imputer = SimpleImputer(strategy='mean')
    df['tenure'] = tenure_imputer.fit_transform(df[['tenure']])

    return df


def basic_preparation(df, good_columns):
    # Drop unnecessary columns
    df = drop_unnecessary_columns(df, good_columns)

    # Handle missing values
    df = handle_missing_values(df)

    return df


if __name__ == "__main__":
    good_columns = [
        'customerID', 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'tenure', 'PhoneService',
        'MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
        'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract', 'PaperlessBilling',
        'PaymentMethod', 'MonthlyCharges', 'TotalCharges'
    ]

    df1 = load_data('/data/raw/database_input.csv')
    df2 = load_data('/data/raw/database_input2.csv')
    df3 = load_data('/data/raw/database_input3.csv')

    df1 = basic_preparation(df1, good_columns)
    df2 = basic_preparation(df2, good_columns)
    df3 = basic_preparation(df3, good_columns)

    df1.to_csv('/data/processed/prepared_database_input.csv', index=False)
    df2.to_csv('/data/processed/prepared_database_input2.csv', index=False)
    df3.to_csv('/data/processed/prepared_database_input3.csv', index=False)

