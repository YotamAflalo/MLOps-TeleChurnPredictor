CREATE TABLE IF NOT EXISTS batch_input (
    customerID VARCHAR(255),
    gender VARCHAR(10),
    SeniorCitizen BOOLEAN,
    Partner VARCHAR(5),
    Dependents VARCHAR(5),
    tenure INTEGER,
    PhoneService VARCHAR(5),
    MultipleLines VARCHAR(20),
    InternetService VARCHAR(20),
    OnlineSecurity VARCHAR(20),
    OnlineBackup VARCHAR(20),
    DeviceProtection VARCHAR(20),
    TechSupport VARCHAR(20),
    StreamingTV VARCHAR(20),
    StreamingMovies VARCHAR(20),
    Contract VARCHAR(20),
    PaperlessBilling VARCHAR(5),
    PaymentMethod VARCHAR(30),
    MonthlyCharges DECIMAL(8,2),
    TotalCharges DECIMAL(8,2)
);

CREATE TABLE IF NOT EXISTS batch_output (
    customerID VARCHAR(255),
    prediction BOOLEAN,
    probability DECIMAL(8,6)
);