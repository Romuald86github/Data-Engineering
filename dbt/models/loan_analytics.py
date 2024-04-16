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

# Fetch and print results
results = cursor.fetchall()
for row in results:
    print(row)

# Close connection
conn.close()
