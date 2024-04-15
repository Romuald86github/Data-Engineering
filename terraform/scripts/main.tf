provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = "us-central1"
}

# Create a Google Cloud Storage bucket
resource "google_storage_bucket" "data_lake_bucket" {
  name     = "loan-data-lake"
  location = "US"
}

# Create a BigQuery dataset
resource "google_bigquery_dataset" "loan_dataset" {
  dataset_id                  = "loan_data"
  friendly_name               = "Loan Data"
  description                 = "Dataset for loan data"
  location                    = "US"
  default_table_expiration_ms = "3600000"
}
