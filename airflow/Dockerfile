# Use the official Airflow image as a parent image
FROM apache/airflow:latest

# Copy the DAGs into the container
COPY ./scripts/airflow_dags /usr/local/airflow/dags

# Expose Airflow webserver port
EXPOSE 8080
