import os
import json
import requests
from kafka import KafkaProducer
from google.cloud import storage

# Kafka configurations
bootstrap_servers = '<KAFKA_BOOTSTRAP_SERVER>'
topic_name = '<KAFKA_TOPIC>'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# GCS configurations
bucket_name = 'your-gcs-bucket-name'
file_name = 'datafile.csv'

# GitHub repo configurations
github_repo_url = 'https://raw.githubusercontent.com/yourusername/yourrepository/main/datafile.csv'

# Initialize GCS client
storage_client = storage.Client()

def fetch_data_from_github(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Failed to fetch data from {url}")
        return None

def publish_to_kafka(data):
    producer.send(topic_name, value=data.encode('utf-8'))
    producer.flush()

def upload_to_gcs(bucket_name, file_name, data):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data)

if __name__ == "__main__":
    # Fetch data from GitHub
    raw_data = fetch_data_from_github(github_repo_url)
    
    if raw_data:
        # Publish data to Kafka
        publish_to_kafka(raw_data)
        print("Data published to Kafka topic successfully.")
        
        # Upload data to GCS
        upload_to_gcs(bucket_name, file_name, raw_data)
        print(f"Data uploaded to GCS bucket {bucket_name} successfully.")
