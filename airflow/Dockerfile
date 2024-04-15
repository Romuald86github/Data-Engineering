# Use the official Airflow image as a parent image
FROM apache/airflow:2.2.0-python3.8

# Copy the DAGs into the container
COPY airflow_dags /usr/local/airflow/dags

# Expose Airflow webserver port
EXPOSE 8080
