# JM Comment: S3 and Iceberg Outputs
output "bucket_name" {
  value = aws_s3_bucket.data_lake.id
}

output "bucket_arn" {
  value = aws_s3_bucket.data_lake.arn
}

output "model_bucket_name" {
  value = aws_s3_bucket.model_store.id
}

output "model_bucket_arn" {
  value = aws_s3_bucket.model_store.arn
}

output "kms_key_arn" {
  value = aws_kms_key.storage_key.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.iceberg_catalog.name
}
