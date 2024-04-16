import psycopg2 # type: ignore
from pyspark.sql import SparkSession # type: ignore

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LoadToPostgres") \
    .getOrCreate()

# Read transformed data from GCS
df_transformed = spark.read.csv("gs://YOUR_GCP_BUCKET/data_transformed.csv", header=True)

# Define PostgreSQL connection parameters
db_params = {
    'host': 'YOUR_POSTGRES_HOST',
    'port': 'YOUR_POSTGRES_PORT',
    'dbname': 'YOUR_POSTGRES_DBNAME',
    'user': 'YOUR_POSTGRES_USER',
    'password': 'YOUR_POSTGRES_PASSWORD'
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)

# Create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS loan_table (
    loanee_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER NOT NULL,
    gender TEXT NOT NULL,
    amount_borrowed REAL NOT NULL,
    date_borrowed TEXT NOT NULL,
    expected_repayment_date TEXT NOT NULL,
    date_repaid TEXT,
    amount_to_be_repaid REAL NOT NULL,
    employment_status TEXT NOT NULL,
    income REAL NOT NULL,
    credit_score INTEGER NOT NULL,
    loan_purpose TEXT NOT NULL,
    loan_type TEXT NOT NULL,
    DefaultStatus TEXT,
    interest_rate REAL NOT NULL,
    loan_term INTEGER NOT NULL,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    zip_code TEXT NOT NULL,
    country TEXT NOT NULL,
    email TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    marital_status TEXT NOT NULL,
    dependents INTEGER NOT NULL,
    education_level TEXT NOT NULL,
    employer TEXT,
    job_title TEXT,
    years_employed INTEGER
);
"""
with conn.cursor() as cursor:
    cursor.execute(create_table_query)
    conn.commit()

# Write data to PostgreSQL
df_transformed.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{db_params['host']}:{db_params['port']}/{db_params['dbname']}") \
    .option("dbtable", "loan_table") \
    .option("user", db_params['user']) \
    .option("password", db_params['password']) \
    .mode("overwrite") \
    .save()

# Close the connection
conn.close()

# Stop Spark session
spark.stop()
