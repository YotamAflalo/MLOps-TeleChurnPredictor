import unittest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# Import the functions to test
from src.models.batch.beam_processing import run, parse_csv_line, TransformData, Predict

class TestBeamProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a temporary directory for test files
        cls.test_dir = tempfile.mkdtemp()
        cls.input_dir = os.path.join(cls.test_dir, 'data', 'batch_input')
        cls.output_dir = os.path.join(cls.test_dir, 'data', 'batch_results')
        os.makedirs(cls.input_dir, exist_ok=True)
        os.makedirs(cls.output_dir, exist_ok=True)

        # Create mock input files
        cls.create_mock_file('original_dataset.csv')
        cls.create_mock_file('database_input.csv')
        cls.create_mock_file('database_input2.csv')

    @classmethod
    def tearDownClass(cls):
        # Remove the temporary directory and its contents
        shutil.rmtree(cls.test_dir)

    @classmethod
    def create_mock_file(cls, filename):
        file_path = os.path.join(cls.input_dir, filename)
        with open(file_path, 'w') as f:
            f.write("Contract,tenure,TotalCharges,PhoneService\n")
            f.write("Month-to-month,12,1000.0,Yes\n")
            f.write("One year,24,2000.0,No\n")

    @patch('src.models.batch.beam_processing.pickle.load')
    def test_run_function(self, mock_pickle_load):
        # Mock the model
        mock_model = MagicMock()
        mock_model.predict.return_value = [0, 1]  # Mock predictions
        mock_pickle_load.return_value = mock_model

        # Run the pipeline
        with patch('src.models.batch.beam_processing.INPUT_DIR', self.input_dir), \
             patch('src.models.batch.beam_processing.OUTPUT_DIR', self.output_dir), \
             patch('src.models.batch.beam_processing.MODEL_PATH', 'mock_model_path'):
            run(input_type='csv', output_type='csv')

        # Check if output files were created
        output_files = os.listdir(self.output_dir)
        self.assertTrue(any(file.startswith('output_') and file.endswith('.csv') for file in output_files))

    def test_parse_csv_line(self):
        line = "Month-to-month,12,1000.0,Yes"
        headers = ["Contract", "tenure", "TotalCharges", "PhoneService"]
        result = parse_csv_line(line, headers)
        expected = {
            "Contract": "Month-to-month",
            "tenure": "12",
            "TotalCharges": "1000.0",
            "PhoneService": "Yes"
        }
        self.assertEqual(result, expected)

    def test_transform_data(self):
        with TestPipeline() as p:
            input_data = [
                {"Contract": "Month-to-month", "tenure": "12", "TotalCharges": "1000.0", "PhoneService": "Yes"}
            ]
            pcoll = p | beam.Create(input_data)
            output = pcoll | beam.ParDo(TransformData())
            
            assert_that(output, equal_to([
                {
                    "Contract": "Month-to-month",
                    "tenure": "12",
                    "TotalCharges": 1000.0,
                    "PhoneService": 1,
                    "Month-to-month": 1,
                    "One year": 0,
                    "Two year": 0
                }
            ]))

    @patch('pickle.load')
    def test_predict(self, mock_pickle_load):
        mock_model = MagicMock()
        mock_model.predict.return_value = [1]
        mock_pickle_load.return_value = mock_model

        with TestPipeline() as p:
            input_data = [
                {
                    "Contract": "Month-to-month",
                    "tenure": "12",
                    "TotalCharges": 1000.0,
                    "PhoneService": 1,
                    "Month-to-month": 1,
                    "One year": 0,
                    "Two year": 0
                }
            ]
            pcoll = p | beam.Create(input_data)
            output = pcoll | beam.ParDo(Predict(mock_model))
            
            assert_that(output, equal_to([
                {
                    "Contract": "Month-to-month",
                    "tenure": "12",
                    "TotalCharges": 1000.0,
                    "PhoneService": 1,
                    "Month-to-month": 1,
                    "One year": 0,
                    "Two year": 0,
                    "prediction": 0
                }
            ]))

if __name__ == '__main__':
    unittest.main()