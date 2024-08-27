# MLOps Mid-Project: ML Model Deployment for Customer Churn Prediction

This project implements a machine learning model for customer churn prediction, utilizing FastAPI and Apache Beam. It includes a comprehensive MLOps pipeline with monitoring, batch processing, and CI/CD integration.

## Features

- FastAPI application for single predictions
- Apache Beam implementation for batch processing
- Monitoring of API and machine with Prometheus
- Error monitoring and alerting with Grafana and Grafana Standing Dashboard
- Automated scheduler for batch processing
- PostgreSQL database for prediction and data storage
- Whylogs data drift monitoring
- Full CI/CD pipeline and testing

## Architecture

1. **Data Collection**: Sources include CSV files in `data/raw` or database queries (fetching only unpredicted data based on the timestamp of the last prediction)
2. **Data Preprocessing**: Data cleaning and transformation for model input (implemented in `beam_preprocessing.py`)
3. **Model Execution**: Running the RandomForestClassifier model on preprocessed data
4. **Output**: Processed data and predictions saved in the `data/batch_results` folder and/or database

## Prerrequisites

- Docker Desktop 
- Git 
- Github Account


## Getting Started

### Running the Full Service

1. Navigate to the docker directory:
   ```
   cd docker
   ```

2. Build the Docker images:
   ```
   docker-compose build
   ```

3. Start the services:
   ```
   docker-compose up
   ```

4. Access the API documentation at [http://localhost:8005/docs](http://localhost:8005/docs)

### Testing the API

Use the `/predict/` POST endpoint with the following example body:

```json
{
  "TotalCharges": "1889.5",
  "Contract": "One year",
  "PhoneService": "Yes",
  "tenure": 34
}
```

Expected response: 
```json
{
  "prediction": 0
}
```
(Indicates the client is not likely to churn soon)


## Batch Processing

The batch processing pipeline utilizes Apache Beam for efficient data processing. It runs daily at 12 PM, performing the following steps:

1. Data retrieval from database or CSV files 
2. Data preprocessing 
3. Model execution using the pickled RandomForestClassifier
4. Saving results back to the database or file system

Configure the batch job settings in the ________________________ file.

## Real-time API

The FastAPI application provides real-time predictions for the marketing server. It uses the same preprocessing steps and model as the batch process to ensure consistency.

## Monitoring and Alerting

### Prometheus

Prometheus is used to collect metrics from both the API and the batch processing pipeline. Key metrics include:
- Prediction volumes
- Response times
- Error rates
- Model performance metrics

### Grafana

1. Access Grafana at [http://localhost:3000/](http://localhost:3000/)
2. Navigate to "Dashboards"
3. Explore the pre-configured dashboards for:
   - API performance
   - Batch processing metrics
   - Model performance over time
   - Data drift indicators

![Grafana Dashboard](dashboard.png)

Alerts are configured in Grafana to notify of any anomalies or issues in the system.

## Database

PostgreSQL is used for storing predictions and raw data.

## Data Drift Monitoring

Whylogs is implemented for data drift detection. Monitor drift metrics through the Grafana dashboard or custom reports generated in the `reports/` directory.

## CI/CD Pipeline

The project includes a full CI/CD pipeline configured with GitHub Actions. View the workflow files in the `.github/workflows/` directory.

## Configuration

To modify input parameters or other configurations, please refer to the configuration files in the `config/` directory.


## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments


For more information or support, please open an issue in the GitHub repository.



 
