import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import PAssert
from unittest.mock import MagicMock
import pandas as pd

# Assuming the DoFns and helper functions from your original code are imported or defined in this file
# e.g., discard_incomplete, TransformData, Predict, etc.

class TestMyPipeline(unittest.TestCase):
    def setUp(self):
        # Mock the model instead of loading it
        self.model = MagicMock()
        self.model.predict.return_value = [1]  # Always return 1 for testing

    def test_pipeline(self):
        # Define your in-memory test data
        input_data = [
            "1,123,Yes,Month-to-month,30,10.0",
            "2,456,No,One year,24,200.0",
            "3,789,Yes,Two year,60,3000.0",
        ]

        # Expected output after processing with mocked prediction
        expected_output = [
            {'customerID': '1', 'tenure': '123', 'PhoneService': 1, 'Contract': 'Month-to-month', 'TotalCharges': 10.0, 'Month-to-month': 1, 'One year': 0, 'Two year': 0, 'prediction': 1},
            {'customerID': '2', 'tenure': '456', 'PhoneService': 0, 'Contract': 'One year', 'TotalCharges': 200.0, 'Month-to-month': 0, 'One year': 1, 'Two year': 0, 'prediction': 1},
            {'customerID': '3', 'tenure': '789', 'PhoneService': 1, 'Contract': 'Two year', 'TotalCharges': 3000.0, 'Month-to-month': 0, 'One year': 0, 'Two year': 1, 'prediction': 1},
        ]

        with TestPipeline() as p:
            output = (
                p
                | 'CreateInputData' >> beam.Create(input_data)
                | 'SplitData' >> beam.Map(lambda x: x.split(','))
                | 'FormatToDict' >> beam.Map(lambda x: {"customerID": x[1], "tenure": x[2],
                                                        "PhoneService": x[3], "Contract": x[4], "TotalCharges": x[5]})
                | 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
                | 'TransformData' >> beam.ParDo(TransformData())
                | 'Predict' >> beam.ParDo(Predict(self.model))
            )

            PAssert.that(output).contains_in_any_order(expected_output)

if __name__ == '__main__':
    unittest.main()