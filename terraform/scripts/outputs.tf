output "bucket_name" {
  value = google_storage_bucket.data_lake_bucket.name
}

output "dataset_id" {
  value = google_bigquery_dataset.loan_dataset.dataset_id
}
