import psycopg2
from psycopg2 import sql

def create_tables():
    # Connection parameters
    dbname = "api_logs"
    user = "user"
    password = "password"
    host = "db"
    port = "5432"

    # Connect to the database
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Create a cursor object
    cur = conn.cursor()

    try:
        # Define your table creation SQL statements here
        tables = [
            """
            CREATE TABLE IF NOT EXISTS batch_input (
    customerID SERIAL PRIMARY KEY,
    Contract VARCHAR(50),
    tenure INTEGER,
    PhoneService BOOLEAN,
    TotalCharges FLOAT,
    timestamp_column TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

    CREATE TABLE IF NOT EXISTS batch_output (
        customerID VARCHAR(50) PRIMARY KEY,
        prediction FLOAT NOT NULL
);
            """
            # Add more CREATE TABLE statements as needed
        ]

        # Execute each CREATE TABLE statement
        for table in tables:
            cur.execute(sql.SQL(table))

        # Commit the changes
        conn.commit()
        print("Tables created successfully")

    except (Exception, psycopg2.Error) as error:
        print(f"Error creating tables: {error}")

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    create_tables()
