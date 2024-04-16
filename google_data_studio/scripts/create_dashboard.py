import pandas as pd # type: ignore
import matplotlib.pyplot as plt # type: ignore
from google.cloud import bigquery
from google.oauth2 import service_account # type: ignore
from googleapiclient.discovery import build # type: ignore

# Authenticate with Google Data Studio API

# Path to the service account JSON key file
SERVICE_ACCOUNT_FILE = 'path_to_service_account.json'

# Authenticate with the service account
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/datastudio"]
)

service = build('datastudio', 'v1', credentials=credentials)

# Initialize BigQuery client
client = bigquery.Client()

# Define the BigQuery datasets and tables
dataset_id = 'your_dataset_id'
loan_table_id = 'loan_analytics_output'
payment_table_id = 'PaymentAnalytics_output'

# Query to fetch data for charts
loan_query = """
SELECT *
FROM `{}.{}`
""".format(dataset_id, loan_table_id)

payment_query = """
SELECT *
FROM `{}.{}`
""".format(dataset_id, payment_table_id)

# Execute the queries
loan_df = client.query(loan_query).to_dataframe()
payment_df = client.query(payment_query).to_dataframe()

# Create the charts
# (1) On-time Repayment Status
on_time_data = loan_df['PaidOnTime'].value_counts()
plt.figure(figsize=(6, 6))
on_time_data.plot(kind='pie', autopct='%1.1f%%')
plt.title('On-time Repayment Status')
plt.show()
plt.close()

# (2) Loan Delay Status
loan_delay_data = loan_df['LateTime'].value_counts()
plt.figure(figsize=(6, 6))
loan_delay_data.plot(kind='pie', autopct='%1.1f%%')
plt.title('Loan Delay Status')
plt.show()
plt.close()

# (3) Total Loan Status
total_loan_data = payment_df['DefaultStatus'].value_counts()
plt.figure(figsize=(6, 6))
total_loan_data.plot(kind='pie', autopct='%1.1f%%')
plt.title('Total Loan Status')
plt.show()
plt.close()

# (4) Monthly Loan Repayment
loan_df['PaidMonth'] = pd.to_datetime(loan_df['date_repaid']).dt.month_name()
monthly_repayment_data = loan_df.groupby('PaidMonth')['amount_to_be_repaid'].agg(['count', 'sum'])
plt.figure(figsize=(10, 6))
monthly_repayment_data['count'].plot(kind='bar', color='blue', label='Number of Loans Repaid')
monthly_repayment_data['sum'].plot(kind='bar', color='green', label='Total Amount Repaid')
plt.title('Monthly Loan Repayment')
plt.xlabel('Month')
plt.ylabel('Count/Amount')
plt.show()
plt.close()

# (5) Loan Default Status
loan_default_data = payment_df['DefaultStatus'].value_counts()
plt.figure(figsize=(6, 6))
loan_default_data.plot(kind='pie', autopct='%1.1f%%')
plt.title('Loan Default Status')
plt.show()
plt.close()

# Define the dashboard body
dashboard_body = {
    "name": "Loan_Repayment_Dashboard",
    "type": "DASHBOARD",
    "credentials": {
        "name": "your_service_account_name",
        "file": "path_to_service_account.json"
    },
    "components": [
        {
            "id": "chart_1",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "PaidOnTime",
                    "metric": "COUNT(PaidOnTime)"
                }
            }
        },
        {
            "id": "chart_2",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "LateTime",
                    "metric": "COUNT(LateTime)"
                }
            }
        },
        {
            "id": "chart_3",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "DefaultStatus",
                    "metric": "COUNT(DefaultStatus)"
                }
            }
        },
        {
            "id": "chart_4",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "BAR",
                "fields": {
                    "dimension": "PaidMonth",
                    "metric": "SUM(amount_to_be_repaid)"
                }
            }
        },
        {
            "id": "chart_5",
            "type": "CHART",
            "dataSource": "your_data_source_id",
            "options": {
                "chartType": "PIE",
                "fields": {
                    "dimension": "DefaultStatus",
                    "metric": "COUNT(DefaultStatus)"
                }
            }
        }
    ]
}

# Make the API request to create the dashboard
request = service.reports().create(body=dashboard_body)
response = request.execute()

# Print the response
print(response)
