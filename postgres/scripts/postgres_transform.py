import psycopg2 # type: ignore

# Database connection parameters
db_params = {
    'host': 'YOUR_POSTGRES_HOST',
    'port': 'YOUR_POSTGRES_PORT',
    'dbname': 'YOUR_POSTGRES_DBNAME',
    'user': 'YOUR_POSTGRES_USER',
    'password': 'YOUR_POSTGRES_PASSWORD'
}

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Check loan_table schema
cursor.execute("""
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'loan_table';
""")
columns = cursor.fetchall()

# Define expected loan_table schema
expected_columns = [
    'loanee_id SERIAL PRIMARY KEY',
    'name TEXT NOT NULL',
    'age INTEGER NOT NULL',
    'gender TEXT NOT NULL',
    'amount_borrowed REAL NOT NULL',
    'date_borrowed TEXT NOT NULL',
    'expected_repayment_date TEXT NOT NULL',
    'date_repaid TEXT',
    'amount_to_be_repaid REAL NOT NULL',
    'employment_status TEXT NOT NULL',
    'income REAL NOT NULL',
    'credit_score INTEGER NOT NULL',
    'loan_purpose TEXT NOT NULL',
    'loan_type TEXT NOT NULL',
    'interest_rate REAL NOT NULL',
    'loan_term INTEGER NOT NULL',
    'address TEXT NOT NULL',
    'city TEXT NOT NULL',
    'state TEXT NOT NULL',
    'zip_code TEXT NOT NULL',
    'country TEXT NOT NULL',
    'email TEXT NOT NULL',
    'phone_number TEXT NOT NULL',
    'marital_status TEXT NOT NULL',
    'dependents INTEGER NOT NULL',
    'education_level TEXT NOT NULL',
    'employer TEXT',
    'job_title TEXT',
    'years_employed INTEGER'
]

# Validate schema
if set(expected_columns) != set([f"{col[0]} {col[1]}" for col in columns]):
    print("Error: loan_table schema does not match the expected schema.")
    cursor.close()
    conn.close()
    exit()

# Create RepaymentHistory table
create_table_query = """
CREATE TABLE IF NOT EXISTS RepaymentHistory (
    loanee_id SERIAL PRIMARY KEY,
    PaidOnTime TEXT,
    LateTime TEXT,
    PaidMonth TEXT,
    amount_paid REAL
);
"""
cursor.execute(create_table_query)
conn.commit()

# Perform transformation and insert data into RepaymentHistory
transform_query = """
INSERT INTO RepaymentHistory (loanee_id, PaidOnTime, LateTime, PaidMonth, amount_paid)
SELECT 
    loanee_id,
    CASE 
        WHEN date_repaid <= expected_repayment_date THEN 'Yes'
        ELSE 'No'
    END AS PaidOnTime,
    CASE 
        WHEN date_repaid IS NOT NULL AND date_repaid > expected_repayment_date THEN '> 7'
        WHEN date_repaid IS NOT NULL AND date_repaid <= expected_repayment_date THEN '< 7'
        ELSE NULL
    END AS LateTime,
    CASE 
        WHEN date_repaid IS NOT NULL THEN EXTRACT(MONTH FROM date_repaid)
        ELSE NULL
    END AS PaidMonth,
    amount_to_be_repaid AS amount_paid
FROM loan_table;
"""

cursor.execute(transform_query)
conn.commit()

# Close the connection
cursor.close()
conn.close()
