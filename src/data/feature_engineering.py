import pandas as pd
from data_preparation import basic_preparation
from feature_engineering import map_categorical, create_features, one_hot_encode


def load_data(file_path):
    return pd.read_csv(file_path)


def map_categorical(df):
    # Example: Mapping PhoneService to binary values
    df['PhoneService'] = df['PhoneService'].map({'Yes': 1, 'No': 0})

    return df


def one_hot_encode(df, columns):
    encoder = OneHotEncoder(sparse=False, drop='first')
    encoded_columns = pd.DataFrame(encoder.fit_transform(df[columns]), columns=encoder.get_feature_names_out(columns))
    df = df.drop(columns=columns)
    df = df.join(encoded_columns)

    return df


def create_features(df):
    # Example: Creating a feature indicating tenure in years
    df['TenureYears'] = df['tenure'] / 12.0 #why we need to do it? it is not done in the Data preparation ipynb. The model does not expect this

    # One-hot encoding for the Contract column
    df = one_hot_encode(df, ['Contract'])

    return df


# if __name__ == "__main__":
#     df1 = load_data('/data/processed/prepared_database_input.csv')
#     df2 = load_data('/data/processed/prepared_database_input2.csv')
#     df3 = load_data('/data/processed/prepared_database_input3.csv')

#     df1 = map_categorical(df1)
#     df2 = map_categorical(df2)
#     df3 = map_categorical(df3)

#     df1 = create_features(df1)
#     df2 = create_features(df2)
#     df3 = create_features(df3)

#     df1.to_csv('/data/processedfeatured_database_input.csv', index=False)
#     df2.to_csv('/data/processedfeatured_database_input2.csv', index=False)
#     df3.to_csv('/data/processedfeatured_database_input3.csv', index=False)