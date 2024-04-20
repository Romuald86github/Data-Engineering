import pandas as pd # type: ignore
import matplotlib.pyplot as plt # type: ignore
from google.cloud import bigquery

def create_charts():
    # Initialize BigQuery client
    client = bigquery.Client()

    # Define the BigQuery datasets and tables
    dataset_id = 'your_dataset_id'
    loan_table_id = 'PaymentAnalytics'   
    payment_table_id =  'loan_analytics'
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
    on_time_data = loan_df['PaidOnTime'].value_counts()
    loan_delay_data = loan_df['LateTime'].value_counts()
    total_loan_data = payment_df["TotalAmountPaid"].sum()
    loan_df['PaidMonth'] = pd.to_datetime(loan_df['date_repaid']).dt.month_name()
    monthly_repayment_data = loan_df.groupby('PaidMonth')['amount_to_be_repaid'].agg(['count', 'sum'])
    loan_default_data = loan_df['DefaultStatus'].value_counts()

