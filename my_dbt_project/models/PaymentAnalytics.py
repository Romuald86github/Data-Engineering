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

# Create the 'PaymentAnalytics' table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS PaymentAnalytics (
    PaidOnTime VARCHAR(3),
    LateTime VARCHAR(3),
    PaidMonth INT,
    date_repaid,
    AmountPaid FLOAT,
    DefaultStatus VARCHAR(3)
);
"""
cursor.execute(create_table_query)
conn.commit()

# Define the SQL query with Jinja templating
sql_query = """
WITH payment_analytics AS (
  SELECT
    CASE 
      WHEN date_repaid <= expected_repayment_date THEN 'Yes'
      ELSE 'No'
    END AS "PaidOnTime",
    CASE
      WHEN date_trunc('day', date_repaid) <= expected_repayment_date AND date_repaid > expected_repayment_date THEN '< 7'
      ELSE '> 7'
    END AS "LateTime",
    EXTRACT(MONTH FROM date_repaid) AS "PaidMonth",
    SUM(amount_to_be_repaid) AS "AmountPaid",
    CASE 
      WHEN date_repaid > expected_repayment_date THEN 'Yes'
      ELSE 'No'
    END AS "DefaultStatus"
  FROM
    loan_table
  WHERE
    date_repaid IS NOT NULL
  GROUP BY
    "PaidOnTime",
    "LateTime",
    "PaidMonth",
    "DefaultStatus"
)

INSERT INTO PaymentAnalytics
SELECT * FROM payment_analytics;
"""

# Execute SQL query
cursor.execute(sql_query)
conn.commit()

# Close connection
conn.close()
