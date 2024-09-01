import unittest
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import shutil
import os
import sys
import pickle

# Add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# Import the functions to test
from src.models.batch.beam_processing import run, parse_csv_line, TransformData, Predict

class MockModel:
    def predict(self, data):
        return [0] * len(data)

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
        cls.create_mock_file('test_input.csv')

    @classmethod
    def tearDownClass(cls):
        # Remove the temporary directory and its contents
        shutil.rmtree(cls.test_dir)

    @classmethod
    def create_mock_file(cls, filename):
        file_path = os.path.join(cls.input_dir, filename)
        with open(file_path, 'w') as f:
            f.write("Contract,tenure,TotalCharges,PhoneService,customerID\n")
            f.write("Month-to-month,12,1000.0,Yes,12345\n")
            f.write("One year,24,2000.0,No,67890\n")

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

    @patch('apache_beam.Pipeline')
    def test_transform_data(self, mock_pipeline):
        input_data = [
            {"customerID": "12345","Contract": "Month-to-month", "tenure": "12", "TotalCharges": "1000.0", "PhoneService": "Yes"}
        ]
        transform = TransformData()
        result = list(transform.process(input_data[0]))
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['Contract'], "Month-to-month")
        self.assertEqual(result[0]['TotalCharges'], 1000.0)
        self.assertEqual(result[0]['PhoneService'], 1)
        self.assertEqual(result[0]['Month-to-month'], 1)
        self.assertEqual(result[0]['One year'], 0)
        self.assertEqual(result[0]['Two year'], 0)

    @patch('apache_beam.Pipeline')
    def test_predict(self, mock_pipeline):
        mock_model = MockModel()
        predict = Predict(mock_model)
        
        input_data = {
            "customerID": "12345",
            "Contract": "Month-to-month",
            "tenure": "12",
            "TotalCharges": 1000.0,
            "PhoneService": 1,
            "Month-to-month": 1,
            "One year": 0,
            "Two year": 0
        }
        
        result = list(predict.process(input_data))
        
        self.assertEqual(len(result), 1)
        self.assertIn('prediction', result[0])
        self.assertEqual(result[0]['prediction'], 0)

    # @patch('src.models.batch.beam_processing.pickle.load')
    # @patch('builtins.open', new_callable=mock_open)
    # @patch('apache_beam.Pipeline')
    # def test_run_function(self, mock_pipeline, mock_file, mock_pickle_load):
    #     # Mock the model
    #     mock_model = MockModel()
    #     mock_pickle_load.return_value = mock_model

    #     # Run the pipeline
    #     with patch('src.models.batch.beam_processing.INPUT_DIR', self.input_dir), \
    #          patch('src.models.batch.beam_processing.OUTPUT_DIR', self.output_dir), \
    #          patch('src.models.batch.beam_processing.MODEL_PATH', 'mock_model_path'):
    #         run(input_type='csv', output_type='csv')

    #     # Check if output files were created
    #     output_files = os.listdir(self.output_dir)
    #     self.assertTrue(any(file.startswith('output_') and file.endswith('.csv') for file in output_files))

if __name__ == '__main__':
    unittest.main()