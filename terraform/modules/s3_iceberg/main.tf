# JM Comment: Storage and Iceberg Catalog Infrastructure
data "aws_caller_identity" "current" {}

# KMS Key for Zero-Trust encryption at rest
resource "aws_kms_key" "storage_key" {
  description             = "KMS key for Iceberg and Ray data encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true

  tags = {
    Environment = var.environment
  }
}

# Primary S3 Data Bucket (Raw datasets, Iceberg warehouse files, Ray object spills)
# JM Comment: Deterministic naming using AWS Account ID ensures zero configuration drift across runs
resource "aws_s3_bucket" "data_lake" {
  bucket        = "${var.bucket_name_prefix}-lake-${data.aws_caller_identity.current.account_id}"
  force_destroy = true

  tags = {
    Name        = "${var.bucket_name_prefix}-lake"
    Environment = var.environment
  }
}

# Enforce S3 Bucket Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.storage_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# Block all public access (Zero-Trust security)
resource "aws_s3_bucket_public_access_block" "block_public" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket versioning for Iceberg transactional safety
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle rule to clean up ephemeral Ray object spills after 3 days
resource "aws_s3_bucket_lifecycle_configuration" "ray_spill_cleanup" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "ray-object-spill-expiry"
    status = "Enabled"

    filter {
      prefix = "ray-spill/"
    }

    expiration {
      days = 3
    }
  }
}

# JM Comment: AWS Glue Data Catalog Database for Apache Iceberg
resource "aws_glue_catalog_database" "iceberg_catalog" {
  name        = "corporate_iceberg_db"
  description = "Glue catalog database for Ray pipeline Apache Iceberg tables"

  parameters = {
    "classification" = "iceberg"
  }
}
