# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Install dbt and psycopg2
RUN pip install dbt psycopg2-binary

# Initialize a new DBT project
RUN dbt init my_dbt_project

# Copy the dbt project files into the container
COPY dbt /app/my_dbt_project

# Copy the Python scripts into the container
COPY loan_analytics.py /app
COPY RepaymentAnalytics.py /app

# Define command to run the Python scripts
CMD ["python", "loan_analytics.py"]  # Default to running loan_analytics.py
