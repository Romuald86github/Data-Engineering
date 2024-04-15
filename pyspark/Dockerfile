# Use an official base image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Install OpenJDK 11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless && \
    apt-get clean;

# Install PySpark
RUN pip install pyspark==3.1.2

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /app
COPY python_scripts /app

# Set environment variable for GCP bucket
ENV YOUR_GCP_BUCKET=your_gcp_bucket_name

# Define command to run the application
CMD ["python", "transform_data_pyspark.py"]
