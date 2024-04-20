import psycopg2 # type: ignore

# Initialize PostgreSQL connection
conn = psycopg2.connect(
    dbname="your_database",
    user="your_user",
    password="your_password",
    host="your_host",
    port="your_port"
)
cursor = conn.cursor()

# Create the 'loan_analytics' table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS loan_analytics (
    Month INT,
    TotalLoansPaid INT,
    TotalAmountPaid FLOAT
);
"""
cursor.execute(create_table_query)
conn.commit()

# Define the SQL query with Jinja templating
sql_query = """
WITH monthly_payments AS (
  SELECT
    EXTRACT(MONTH FROM date_repaid) AS "Month",
    COUNT(*) AS "TotalLoansPaid",
    SUM(amount_to_be_repaid) AS "TotalAmountPaid"
  FROM
    loan_table
  WHERE
    date_repaid IS NOT NULL
  GROUP BY
    "Month"
)

INSERT INTO loan_analytics
SELECT
  "Month",
  "TotalLoansPaid",
  "TotalAmountPaid"
FROM
  monthly_payments
ORDER BY
  "Month";
"""

# Execute SQL query
cursor.execute(sql_query)
conn.commit()

# Close connection
conn.close()
