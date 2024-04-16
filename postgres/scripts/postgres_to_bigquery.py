from google.cloud import bigquery
import psycopg2 # type: ignore

# Database connection parameters
db_params = {
    'host': 'YOUR_POSTGRES_HOST',
    'port': 'YOUR_POSTGRES_PORT',
    'dbname': 'YOUR_POSTGRES_DBNAME',
    'user': 'YOUR_POSTGRES_USER',
    'password': 'YOUR_POSTGRES_PASSWORD'
}

# BigQuery parameters
project_id = 'YOUR_GCP_PROJECT_ID'
dataset_id = 'YOUR_BIGQUERY_DATASET_ID'
table_names = ['loan_table', 'RepaymentHistory']

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Initialize PostgreSQL connection
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Load data from PostgreSQL to BigQuery
for table_name in table_names:
    # Fetch data from PostgreSQL
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    # Define BigQuery table schema based on PostgreSQL table schema
    cursor.execute(f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name = '{table_name}';
    """)
    columns = cursor.fetchall()
    
    # Convert PostgreSQL column names and types to BigQuery schema
    table_schema = [
        bigquery.SchemaField(col[0], col[1].upper()) for col in columns
    ]
    
    # Create BigQuery dataset if it doesn't exist
    dataset_ref = client.dataset(dataset_id)
    if not client.get_dataset(dataset_ref, retry=bigquery.DEFAULT_RETRY).exists():
        client.create_dataset(dataset_ref)

    # Create BigQuery table
    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=table_schema)
    if not client.get_table(table_ref, retry=bigquery.DEFAULT_RETRY).exists():
        client.create_table(table)

    # Insert data into BigQuery table
    errors = client.insert_rows_json(table_ref, [dict(zip([col[0] for col in columns], row)) for row in rows])
    
    if errors:
        print(f"Errors occurred while inserting data into {table_name} in BigQuery.")
    else:
        print(f"Data inserted successfully into {table_name} in BigQuery.")

# Close PostgreSQL connection
cursor.close()
conn.close()
