-- Create batch_input table
CREATE TABLE IF NOT EXISTS batch_input (
    customerID SERIAL PRIMARY KEY,
    Contract VARCHAR(50),
    tenure INTEGER,
    PhoneService BOOLEAN,
    TotalCharges FLOAT,
    timestamp_column TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create batch_output table
CREATE TABLE IF NOT EXISTS batch_output (
    customerID VARCHAR(50) PRIMARY KEY,
    prediction FLOAT NOT NULL
);