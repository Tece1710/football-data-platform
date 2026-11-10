# Landing zone for raw API responses
resource "aws_s3_bucket" "raw_file_bucket" {
  bucket = var.raw_bucket_name

  # Allows `terraform destroy` to delete the bucket even when it contains files.
  # Fine for a portfolio project; you would not set this on production data.
  force_destroy = true
}

# Make sure nothing in the bucket can ever be made public
resource "aws_s3_bucket_public_access_block" "raw_file_bucket_access" {
  bucket = aws_s3_bucket.raw_file_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
