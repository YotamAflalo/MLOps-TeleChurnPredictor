import unittest
import pandas as pd
from io import StringIO
from test_data_preparation.py import drop_unnecessary_columns, handle_missing_values, basic_preparation

class TestDataPreparation(unittest.TestCase):
    def setUp(self):
        # Sample CSV data
        self.sample_data = StringIO("""customerID,gender,SeniorCitizen,Partner,Dependents,tenure,PhoneService,MultipleLines,InternetService,OnlineSecurity,OnlineBackup,DeviceProtection,TechSupport,StreamingTV,StreamingMovies,Contract,PaperlessBilling,PaymentMethod,MonthlyCharges,TotalCharges
1,Female,0,Yes,No,5,Yes,No,DSL,Yes,No,No,No,No,No,Month-to-month,Yes,Electronic check,29.85,
2,Male,0,No,No,,Yes,No,DSL,Yes,Yes,No,Yes,No,No,One year,No,Mailed check,56.95,188.65
""")

        self.good_columns = [
            'customerID', 'gender', 'SeniorCitizen', 'Partner', 'Dependents', 'tenure', 'PhoneService',
            'MultipleLines', 'InternetService', 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
            'TechSupport', 'StreamingTV', 'StreamingMovies', 'Contract', 'PaperlessBilling',
            'PaymentMethod', 'MonthlyCharges', 'TotalCharges'
        ]

        self.df = pd.read_csv(self.sample_data)

    def test_drop_unnecessary_columns(self):
        df_dropped = drop_unnecessary_columns(self.df, self.good_columns)
        self.assertListEqual(list(df_dropped.columns), self.good_columns)

    def test_handle_missing_values(self):
        df_handled = handle_missing_values(self.df.copy())

        # Ensure no missing values in Contract, PhoneService, tenure, and TotalCharges columns
        self.assertFalse(df_handled['Contract'].isnull().any())
        self.assertFalse(df_handled['PhoneService'].isnull().any())
        self.assertFalse(df_handled['tenure'].isnull().any())
        self.assertFalse(df_handled['TotalCharges'].isnull().any())

        # Check that the TotalCharges column is converted to float and missing values are filled with 2279
        self.assertEqual(df_handled['TotalCharges'].dtype, float)
        self.assertTrue((df_handled['TotalCharges'] == 2279).any())

    def test_basic_preparation(self):
        df_prepared = basic_preparation(self.df.copy(), self.good_columns)

        # Ensure no missing values in important columns
        self.assertFalse(df_prepared['Contract'].isnull().any())
        self.assertFalse(df_prepared['PhoneService'].isnull().any())
        self.assertFalse(df_prepared['tenure'].isnull().any())
        self.assertFalse(df_prepared['TotalCharges'].isnull().any())

        # Ensure the DataFrame has only the good columns
        self.assertListEqual(list(df_prepared.columns), self.good_columns)

if __name__ == '__main__':
    unittest.main()