output "raw_bucket_name" {
  description = "Name of the raw landing bucket"
  value       = aws_s3_bucket.raw_file_bucket.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the raw landing bucket (needed later for IAM policies)"
  value       = aws_s3_bucket.raw_file_bucket.arn
}
