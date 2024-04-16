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

# Create RepaymentHistory table
create_table_query = """
CREATE TABLE IF NOT EXISTS RepaymentHistory (
    loanee_id SERIAL PRIMARY KEY,
    PaidOnTime TEXT NOT NULL,
    LateTime TEXT NOT NULL,
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
